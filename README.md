# Front Package

Front Package is a go common package.


## Contents

- [front-pkg](#Front Package)
  - [Contents](#contents)
  - [Installation](#installation)
  - [Cache](#cache)
  - [Config](#config)

## Installation

To install front-pkg package, you need to install Go and set your Go workspace first.

1. Init git config with your gitlab config.

```shell script
$ git config --global url."git@gitlab.jryghq.com:".insteadOf "https://gitlab.jryghq.com/"
```

2. The first need [Go](https://golang.org/) installed (**version 1.11+ is required**), then you can use the below Go command to install Gin.

```shell script
$ go env -w GOSUMDB=off
$ go get -u github.com/Darker-D/ddbase
```

3. Import it in your code:

```go
import "github.com/Darker-D/ddbase"
```

## Cache

## Config