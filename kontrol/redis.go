package kontrol

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
	//	"github.com/hashicorp/go-version"
	"github.com/mikif70/kite"
	kontrolprotocol "github.com/mikif70/kite/kontrol/protocol"
	"github.com/mikif70/kite/protocol"
	"strings"
)

// Redis implements the Storage interface
type Redis struct {
	db  *redis.Pool
	log kite.Logger
}

func NewRedis(machines []string, log kite.Logger) *Redis {
	var server string
	if machines == nil || len(machines) == 0 {
		server = "127.0.0.1:6379"
	} else {
		server = machines[0]
	}

	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return &Redis{
		db:  pool,
		log: log,
	}
}

// Delete deletes the given kite from the storage
func (r *Redis) Delete(k *protocol.Kite) error {
	rKey := r.redisKey(k)

	r.log.Debug("DEL %s", rKey)

	conn := r.db.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", rKey)

	return err
}

func (r *Redis) Add(k *protocol.Kite, value *kontrolprotocol.RegisterValue) error {
	rKey := r.redisKey(k)

	r.log.Debug("ADD %s EX %f", rKey, KeyTTL.Seconds())

	valueBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	valueString := string(valueBytes)

	conn := r.db.Get()
	defer conn.Close()

	// Set the kite key.
	// Example "/koding/production/os/0.0.1/sj/kontainer1.sj.koding.com/1234asdf..."
	_, err = conn.Do("SET", rKey, valueString, "EX", KeyTTL.Seconds())
	if err != nil {
		return err
	}

	return nil
}

// Update updates the value for the given kite
func (r *Redis) Update(k *protocol.Kite, value *kontrolprotocol.RegisterValue) error {
	rKey := r.redisKey(k)

	r.log.Debug("UPDATE %s EX %f", rKey, KeyTTL.Seconds())

	valueBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	valueString := string(valueBytes)

	conn := r.db.Get()
	defer conn.Close()

	// Set the kite key.
	// Example "/koding/production/os/0.0.1/sj/kontainer1.sj.koding.com/1234asdf..."
	_, err = conn.Do("SET", rKey, valueString, "EX", KeyTTL.Seconds(), "XX")
	if err != nil {
		return err
	}

	return nil
}

// Upsert inserts or updates the value for the given kite
func (r *Redis) Upsert(kite *protocol.Kite, value *kontrolprotocol.RegisterValue) error {
	r.log.Debug("UPSERT")

	return r.Add(kite, value)
}

func (r *Redis) Get(query *protocol.KontrolQuery) (Kites, error) {

	// We will make a get request to etcd store with this key. So get a "etcd"
	// key from the given query so that we can use it to query from Etcd.
	rKey, err := r.getQueryKey(query)
	if err != nil {
		return nil, err
	}

	r.log.Debug("GET %+v - %+v", query, rKey)

	conn := r.db.Get()
	defer conn.Close()

	repl, err := redis.Values(conn.Do("SCAN", 0, "MATCH", rKey))
	if err != nil {
		r.log.Debug("SCAN error: %s", err)
		return nil, err
	}

	ar := repl[1].([]interface{})

	kites := make(Kites, 0)

	for i := range ar {
		r.log.Debug("READ %s", ar[i])
		get, _ := conn.Do("GET", ar[i])

		var value kontrolprotocol.RegisterValue

		err := json.Unmarshal(get.([]byte), &value)
		if err != nil {
			return nil, err
		}

		kt := r.Kite(string(ar[i].([]byte)))

		r.log.Debug("GET %s", get)
		kts := &protocol.KiteWithToken{
			Kite:  *kt,
			URL:   value.URL,
			KeyID: value.KeyID,
		}
		r.log.Debug("KTS %+v", kts)

		kites = append(kites, kts)
	}

	// Shuffle the list
	kites.Shuffle()

	return kites, nil
}

func (r *Redis) Kite(rKey string) *protocol.Kite {
	kt := strings.Split(rKey, ":")
	kite := &protocol.Kite{
		Username:    kt[1],
		Environment: kt[2],
		Name:        kt[3],
		Version:     kt[4],
		Region:      kt[5],
		Hostname:    kt[6],
		ID:          kt[7],
	}

	return kite
}

func (r *Redis) redisKey(k *protocol.Kite) string {

	return strings.Trim(KitesPrefix, "/") +
		":" + k.Username +
		":" + k.Environment +
		":" + k.Name +
		":" + k.Version +
		":" + k.Region +
		":" + k.Hostname +
		":" + k.ID
}

// getQueryKey returns the etcd key for the query.
func (r *Redis) getQueryKey(q *protocol.KontrolQuery) (string, error) {
	fields := q.Fields()

	if q.Username == "" {
		return "", errors.New("Empty username field")
	}

	// Validate query and build key.
	path := "*"

	empty := false   // encountered with empty field?
	empytField := "" // for error log

	// http://golang.org/doc/go1.3#map, order is important and we can't rely on
	// maps because the keys are not ordered :)
	for _, key := range keyOrder {
		v := fields[key]
		if v == "" {
			empty = true
			empytField = key
			v = "*"
		}

		if empty && v != "*" && v != "" {
			return "", fmt.Errorf("Invalid query. Query option is not set: %s", empytField)
		}

		path = path + v + ":"
	}

	path = strings.TrimSuffix(path, ":")

	return path, nil
}
