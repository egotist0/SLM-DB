module storage

go 1.19

require (
	github.com/stretchr/testify v1.8.2
	go.etcd.io/bbolt v1.3.7
	golang.org/x/sys v0.4.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.etcd.io/bbolt => github.com/egotist0/bbolt v0.0.0-20220315040627-32fed02add8f
