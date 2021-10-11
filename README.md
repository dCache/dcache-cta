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

The plugin should be listed.

To activate the plugin, create an HSM instance using:

    hsm create osm name org.dcache.cta.dcache-cta [-key=value]...

## Acknowledgements

This driver is based and inspired by [`cta-communicator`](https://github.com/lemora/cta-communicator) by _Lea Morschel_ and  [`sapphire`](https://github.com/dCache/sapphire) by _Svenja Meyer_
