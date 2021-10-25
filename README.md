# dCache Nearline Storage Driver for CTA

<img src=".assets/cta+dcache.png" height="165" width="200">

This is nearline storage plugin for dCache.

To compile the plugin, run:

    mvn package

This produces a tarball in the `target` directory containing the plugin.

## Using the plugin with dCache

To use this plugin with dCache, place the directory containing this
file in /usr/local/share/dcache/plugins/ on a dCache pool. Restart
the pool to load the plugin.

To verify that the plugin is loaded, navigate to the pool in the dCache admin
shell and issue the command:

    hsm show providers

The plugin should be listed as **dcache-cta**.

To activate the plugin, create an HSM instance using:

    hsm create osm name dcache-cta [-key=value]...

example:

    hsm create osm cta dcache-cta -cta-user=userA \
         -cta-group=groupA -cta-instance-name=instanceA \
         -cta-frontend-addr=cta-forntend-host:17017 \
         -io-endpoint=a.b.c.d -io-port=1094

The dCache files stored in CTA will have hsm uri in form
```
<hsmType>://<hsmName>/<pnfsid>?arichiveid=<id>
```
Where `id` represents the CTA internal `archiveId`

for example:
```
osm://cta/00001D43C0C086CA459298C634D67F68AB6B?archiveid=8402
```

In the CTA catalog the dCache's `pnfsid`s are referenced as `disk_file_id` field:

```
db => select * from archive_file where disk_file_id = '00001D43C0C086CA459298C634D67F68AB6B';
-[ RECORD 1 ]-------+-------------------------------------
archive_file_id     | 8402
disk_instance_name  | instanceA
disk_file_id        | 00001D43C0C086CA459298C634D67F68AB6B
disk_file_uid       | 1
disk_file_gid       | 1
size_in_bytes       | 10482
checksum_blob       | \x0a08080112043309f498
checksum_adler32    | 2566129971
storage_class_id    | 81
creation_time       | 1635150624
reconciliation_time | 1635150624
is_deleted          | 0
collocation_hint    |
```

> NOTE: dCache-CTA driver doesn't preserve file's ownership, thus values of `uid` and `gid` fields
> contain arbitrary values.

## dCache pool configuration

As CTA has its own scheduler and flush/restore queue the dCache pools should be configured to
submit as much request as possible. Thus grouping and collecting request on the pool side should
be disabled:

```
queue define class -expire=0 -pending=0 -total=0 -open <hsmType> *@<hsmType>
```

### The available configuration options:

| Name | Description | required | default |
| :--- | :--- | ---: | --- |
cta-instance-name | The dCache instance name configured in CTA | yes | -
cta-frontend-addr | The CTA `cta-dcache` endpoint | yes | -
cta-user | The dCache instance associated user in CTA | yes | -
cta-user | The dCache instance associated group in CTA | yes | -
io-endpoint | The hostname or IP offered by dCache for IO by CTA | no | `hostname`
io-port | The TCP port offered by dCache for IO by CTA | no | -

## Acknowledgements

This driver is based and inspired by [`cta-communicator`](https://github.com/lemora/cta-communicator) by _Lea Morschel_ and  [`sapphire`](https://github.com/dCache/sapphire) by _Svenja Meyer_
