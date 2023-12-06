package internal

import "go.uber.org/zap"

//var Logger = log.New(os.Stderr, "Gdb: ", log.LstdFlags|log.Lshortfile)

var Logger = zap.NewExample(zap.AddCaller(), zap.Development())
