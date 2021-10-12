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
