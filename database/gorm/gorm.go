package gorm

import (
	"github.com/Darker-D/ddbase/ecode"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/prometheus"
	"strings"
	"time"
)

// Config mysql config.
type Config struct {
	DSN         string        // data source name.
	Active      int           // pool
	Idle        int           // pool
	IdleTimeout time.Duration // connect max life time.
}

func init() {
	gorm.ErrRecordNotFound = ecode.NothingFound
}

// NewMySQL new db and retry connection when has error.
func NewMySQL(c *Config) (db *gorm.DB) {

	db, err := gorm.Open(mysql.Open(c.DSN), &gorm.Config{
		Logger: logger.Default,
	})
	if err != nil {
		panic(err)
	}

	sdb, err := db.DB()
	if err != nil {
		panic(err)
	}

	// setting mysql conn pool
	sdb.SetMaxIdleConns(c.Idle)
	sdb.SetMaxOpenConns(c.Active)
	sdb.SetConnMaxLifetime(c.IdleTimeout)

	dbName := c.DSN[strings.Index(c.DSN, "/")+1 : strings.Index(c.DSN, "?")]
	// setting mysql prometheus monitor
	err = db.Use(prometheus.New(prometheus.Config{
		DBName: dbName, // use `DBName` as metrics label
		// 已暴露 metrics .
		// RefreshInterval: 15,                          // Refresh metrics interval (default 15 seconds)
		// PushAddr:        "prometheus pusher address", // push metrics if `PushAddr` configured
		// StartServer:     false,                        // start http server to expose metrics
		// HTTPServerPort:  8080,                        // configure http server port, default port 8080 (if you have configured multiple instances, only the first `HTTPServerPort` will be used to start server)
		MetricsCollector: []prometheus.MetricsCollector{
			&prometheus.MySQL{
				// Metrics name prefix, default is `gorm_status_`
				// For example, Threads_running's metric name is `gorm_status_Threads_running`
				Prefix: "gorm_status_",
				// Fetch interval, default use Prometheus's RefreshInterval
				Interval: 100,
				// Select variables from SHOW STATUS, if not set, uses all status variables
				VariableNames: []string{"Threads_running"},
			},
		}, // user defined metrics
	}))
	if err != nil {
		panic(err)
	}

	_ = db.Use(NewTracePlugin())
	return
}
