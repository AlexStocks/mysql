// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net"
)

/*
func init() {
	sql.Register("z-driver", &ZDriver{})
}

type ZDriver struct {
	ctx context.Context
}

func NewZDriver(ctx context.Context) *ZDriver {
	return &ZDriver{ctx: ctx}
}

// Open new Connection.
// See https://github.com/go-sql-driver/mysql#dsn-data-source-name for how
// the DSN string is formatted
func (d ZDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	zConnector := &mysql.ZConnector{}
	zConnector.SetConfig(cfg)

	ctx := d.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return zConnector.Connect(ctx)
}

// OpenConnector implements driver.DriverContext.
func (d ZDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	zConnector := &mysql.ZConnector{}
	cfg.Context = d.ctx
	zConnector.SetConfig(cfg)
	return zConnector, nil
}

func examples() {
    dsn := "esmpmain@tcp(" + "127.0.0.1:13306" + ")/" + utils.DBName + "?charset=utf8&timeout=90s&collation=utf8mb4_unicode_ci"

    // test prepare
	//db, err := zdasDriver.Open(dsn)
	//if err != nil {
	//	t.Fatalf("connect dsn %s, got error %v", dsn, err)
	//}
	//defer db.Close()
	// db.Prepare()

    db, err = sql.Open("z-driver", dsn)
    defer db.Close()
	db.Query("use " + utils.DBName)
}

*/

type ZConnector = connector

func (z *ZConnector) SetConfig(cfg *Config) {
	z.cfg = cfg
}

type connector struct {
	cfg *Config // immutable private copy.
}

const (
	AttachConnection = "Attach-Connection"
)

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var err error

	// New mysqlConn
	mc := &mysqlConn{
		maxAllowedPacket: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
		closech:          make(chan struct{}),
		cfg:              c.cfg,
	}
	mc.parseTime = mc.cfg.ParseTime

	// Connect to Server
	var existFlag bool
	conn := ctx.Value(AttachConnection)
	if conn == nil {
		conn = c.cfg.Context.Value(AttachConnection)
	}
	if conn != nil {
		if tcpConn, ok := conn.(net.Conn); ok {
			existFlag = true
			mc.netConn = tcpConn
		}
	}

	if !existFlag {
		dialsLock.RLock()
		dial, ok := dials[mc.cfg.Net]
		dialsLock.RUnlock()
		if ok {
			dctx := ctx
			if mc.cfg.Timeout > 0 {
				var cancel context.CancelFunc
				dctx, cancel = context.WithTimeout(ctx, c.cfg.Timeout)
				defer cancel()
			}
			mc.netConn, err = dial(dctx, mc.cfg.Addr)
		} else {
			nd := net.Dialer{Timeout: mc.cfg.Timeout}
			mc.netConn, err = nd.DialContext(ctx, mc.cfg.Net, mc.cfg.Addr)
		}

		if err != nil {
			return nil, err
		}
	}

	// Enable TCP Keepalives on TCP connections
	if tc, ok := mc.netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			// Don't send COM_QUIT before handshake.
			mc.netConn.Close()
			mc.netConn = nil
			return nil, err
		}
	}

	// Call startWatcher for context support (From Go 1.8)
	mc.startWatcher()
	if err := mc.watchCancel(ctx); err != nil {
		mc.cleanup()
		return nil, err
	}
	defer mc.finish()

	mc.buf = newBuffer(mc.netConn)

	// Set I/O timeouts
	mc.buf.timeout = mc.cfg.ReadTimeout
	mc.writeTimeout = mc.cfg.WriteTimeout

	// Reading Handshake Initialization Packet
	authData, plugin, err := mc.readHandshakePacket()
	if err != nil {
		mc.cleanup()
		return nil, err
	}

	if plugin == "" {
		plugin = defaultAuthPlugin
	}

	// Send Client Authentication Packet
	authResp, err := mc.auth(authData, plugin)
	if err != nil {
		// try the default auth plugin, if using the requested plugin failed
		errLog.Print("could not use requested auth plugin '"+plugin+"': ", err.Error())
		plugin = defaultAuthPlugin
		authResp, err = mc.auth(authData, plugin)
		if err != nil {
			mc.cleanup()
			return nil, err
		}
	}
	if err = mc.writeHandshakeResponsePacket(authResp, plugin); err != nil {
		mc.cleanup()
		return nil, err
	}

	// Handle response to auth packet, switch methods if possible
	if err = mc.handleAuthResult(authData, plugin); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/internals/en/authentication-fails.html).
		// Do not send COM_QUIT, just cleanup and return the error.
		mc.cleanup()
		return nil, err
	}

	if mc.cfg.MaxAllowedPacket > 0 {
		mc.maxAllowedPacket = mc.cfg.MaxAllowedPacket
	} else {
		// Get max allowed packet size
		maxap, err := mc.getSystemVar("max_allowed_packet")
		if err != nil {
			mc.Close()
			return nil, err
		}
		mc.maxAllowedPacket = stringToInt(maxap) - 1
	}
	if mc.maxAllowedPacket < maxPacketSize {
		mc.maxWriteSize = mc.maxAllowedPacket
	}

	// Handle DSN Params
	err = mc.handleParams()
	if err != nil {
		mc.Close()
		return nil, err
	}

	return mc, nil
}

// Driver implements driver.Connector interface.
// Driver returns &MySQLDriver{}.
func (c *connector) Driver() driver.Driver {
	return &MySQLDriver{}
}

