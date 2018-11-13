# rsync-manager

work repo for rsyncman, included in **eyp-rsync**

## Usage

```
# rsyncman -h
Usage: /usr/bin/rsyncman [-c <config file>] [-b]

-h,--help print this message
-c,--config config file
-b,--syncback sync from destination to origin
```

## config options

### rsyncman section

Global config section

* to
* host-id
* logdir

### job section

Job specific options (can be configured more than one job) Section name is the local path

* ionice
* rsync-path
* exclude
* delete
* remote
* remote-path
* check-file
* expected-fs
* expected-remote-fs

## demo config file

```
[rsyncman]
to=demo@example.com
host-id=DEMOHOST1234
logdir=/var/log/rsyncman.log

[/test_rsync]
ionice="-c2 -n2"
rsync-path="sudo rsync"
exclude = [ "exclude1","exclude2" ]
delete = false
remote="jprats@127.0.0.1"
remote-path="/test_rsync"
check-file=is.mounted
expected-fs=nfs
expected-remote-fs=nfs
```
