// Package kite is a library for creating micro-services.  Two main types
// implemented by this package are Kite for creating a micro-service server
// called "Kite" and Client for communicating with another kites.
package kite

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"
	"github.com/koding/kite/config"
	"github.com/koding/kite/protocol"
	uuid "github.com/satori/go.uuid"
	"gopkg.in/igm/sockjs-go.v2/sockjs"
)

var hostname string

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("kite: cannot get hostname: %s", err.Error()))
	}
}

// Kite defines a single process that enables distributed service messaging
// amongst the peers it is connected. A Kite process acts as a Client and as a
// Server. That means it can receive request, process them, but it also can
// make request to other kites.
//
// Do not use this struct directly. Use kite.New function, add your handlers
// with HandleFunc mehtod, then call Run method to start the inbuilt server (or
// pass it to any http.Handler compatible server)
type Kite struct {
	Config *config.Config

	// Log logs with the given Logger interface
	Log Logger

	// SetLogLevel changes the level of the logger. Default is INFO.
	SetLogLevel func(Level)

	// Contains different functions for authenticating user from request.
	// Keys are the authentication types (options.auth.type).
	Authenticators map[string]func(*Request) error

	// Kontrol keys to trust. Kontrol will issue access tokens for kites
	// that are signed with the private counterpart of these keys.
	// Key data must be PEM encoded.
	trustedKontrolKeys map[string]string

	// Handlers added with Kite.HandleFunc().
	handlers     map[string]*Method // method map for exported methods
	preHandlers  []Handler          // a list of handlers that are executed before any handler
	postHandlers []Handler          // a list of handlers that are executed after any handler

	// MethodHandling defines how the kite is returning the response for
	// multiple handlers
	MethodHandling MethodHandling

	// HTTP muxer
	muxer *mux.Router

	// kontrolclient is used to register to kontrol and query third party kites
	// from kontrol
	kontrol *kontrolClient

	// Handlers to call when a new connection is received.
	onConnectHandlers []func(*Client)

	// Handlers to call before the first request of connected kite.
	onFirstRequestHandlers []func(*Client)

	// Handlers to call when a client has disconnected.
	onDisconnectHandlers []func(*Client)

	// server fields, are initialized and used when
	// TODO: move them to their own struct, just like KontrolClient
	listener  net.Listener
	TLSConfig *tls.Config
	readyC    chan bool // To signal when kite is ready to accept connections
	closeC    chan bool // To signal when kite is closed with Close()

	name    string
	version string
	Id      string // Unique kite instance id
}

// New creates, initialize and then returns a new Kite instance. Version must
// be in 3-digit semantic form. Name is important that it's also used to be
// searched by others.
func New(name, version string) *Kite {
	if name == "" {
		panic("kite: name cannot be empty")
	}

	if digits := strings.Split(version, "."); len(digits) != 3 {
		panic("kite: version must be 3-digits semantic version")
	}

	kiteID := uuid.NewV4()

	l, setlevel := newLogger(name)

	kClient := &kontrolClient{
		readyConnected:  make(chan struct{}),
		readyRegistered: make(chan struct{}),
		registerChan:    make(chan *url.URL, 1),
	}

	k := &Kite{
		Config:             config.New(),
		Log:                l,
		SetLogLevel:        setlevel,
		Authenticators:     make(map[string]func(*Request) error),
		trustedKontrolKeys: make(map[string]string),
		handlers:           make(map[string]*Method),
		preHandlers:        make([]Handler, 0),
		postHandlers:       make([]Handler, 0),
		kontrol:            kClient,
		name:               name,
		version:            version,
		Id:                 kiteID.String(),
		readyC:             make(chan bool),
		closeC:             make(chan bool),
		muxer:              mux.NewRouter(),
	}

	// We change the heartbeat interval from 25 seconds to 10 seconds. This is
	// better for environments such as AWS ELB.
	sockjsOpts := sockjs.DefaultOptions
	sockjsOpts.HeartbeatDelay = 10 * time.Second

	// All sockjs communication is done through this endpoint..
	k.muxer.PathPrefix("/kite").Handler(sockjs.NewHandler("/kite", sockjsOpts, k.sockjsHandler))

	// Add useful debug logs
	k.OnConnect(func(c *Client) { k.Log.Debug("New session: %s", c.session.ID()) })
	k.OnFirstRequest(func(c *Client) { k.Log.Debug("Session %q is identified as %q", c.session.ID(), c.Kite) })
	k.OnDisconnect(func(c *Client) { k.Log.Debug("Kite has disconnected: %q", c.Kite) })

	// Every kite should be able to authenticate the user from token.
	// Tokens are granted by Kontrol Kite.
	k.Authenticators["token"] = k.AuthenticateFromToken

	// A kite accepts requests with the same username.
	k.Authenticators["kiteKey"] = k.AuthenticateFromKiteKey

	// Register default methods and handlers.
	k.addDefaultHandlers()

	return k
}

// Kite returns the definition of the kite.
func (k *Kite) Kite() *protocol.Kite {
	return &protocol.Kite{
		Username:    k.Config.Username,
		Environment: k.Config.Environment,
		Name:        k.name,
		Version:     k.version,
		Region:      k.Config.Region,
		Hostname:    hostname,
		ID:          k.Id,
	}
}

// Trust a Kontrol key for validating tokens.
func (k *Kite) TrustKontrolKey(issuer, key string) {
	k.trustedKontrolKeys[issuer] = key
}

// HandleHTTP registers the HTTP handler for the given pattern into the
// underlying HTTP muxer.
func (k *Kite) HandleHTTP(pattern string, handler http.Handler) {
	k.muxer.Handle(pattern, handler)
}

// HandleHTTPFunc registers the HTTP handler for the given pattern into the
// underlying HTTP muxer.
func (k *Kite) HandleHTTPFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	k.muxer.HandleFunc(pattern, handler)
}

// ServeHTTP helps Kite to satisfy the http.Handler interface. So kite can be
// used as a standard http server.
func (k *Kite) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	k.muxer.ServeHTTP(w, req)
}

func (k *Kite) sockjsHandler(session sockjs.Session) {
	defer session.Close(0, "")

	// This Client also handles the connected client.
	// Since both sides can send/receive messages the client code is reused here.
	c := k.NewClient("")
	c.m.Lock()
	c.session = session
	c.m.Unlock()

	go c.sendHub()
	c.wg.Add(1) // with sendHub we added a new listener

	k.callOnConnectHandlers(c)

	// Run after methods are registered and delegate is set
	c.readLoop()

	c.callOnDisconnectHandlers()
	k.callOnDisconnectHandlers(c)
}

func (k *Kite) OnConnect(handler func(*Client)) {
	k.onConnectHandlers = append(k.onConnectHandlers, handler)
}

// OnFirstRequest registers a function to run when a Kite connects to this Kite.
func (k *Kite) OnFirstRequest(handler func(*Client)) {
	k.onFirstRequestHandlers = append(k.onFirstRequestHandlers, handler)
}

// OnDisconnect registers a function to run when a connected Kite is disconnected.
func (k *Kite) OnDisconnect(handler func(*Client)) {
	k.onDisconnectHandlers = append(k.onDisconnectHandlers, handler)
}

func (k *Kite) callOnConnectHandlers(c *Client) {
	for _, handler := range k.onConnectHandlers {
		handler(c)
	}
}

func (k *Kite) callOnFirstRequestHandlers(c *Client) {
	for _, handler := range k.onFirstRequestHandlers {
		handler(c)
	}
}

func (k *Kite) callOnDisconnectHandlers(c *Client) {
	for _, handler := range k.onDisconnectHandlers {
		handler(c)
	}
}

// RSAKey returns the corresponding public key for the issuer of the token.
// It is called by jwt-go package when validating the signature in the token.
func (k *Kite) RSAKey(token *jwt.Token) (interface{}, error) {
	if k.Config.KontrolKey == "" {
		panic("kontrol key is not set in config")
	}

	issuer, ok := token.Claims["iss"].(string)
	if !ok {
		return nil, errors.New("token does not contain a valid issuer claim")
	}

	if issuer != k.Config.KontrolUser {
		return nil, fmt.Errorf("issuer is not trusted: %s", issuer)
	}

	return []byte(k.Config.KontrolKey), nil
}
