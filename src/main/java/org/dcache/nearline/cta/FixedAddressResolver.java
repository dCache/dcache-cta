package org.dcache.nearline.cta;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A simple {@link NameResolver} for fixed addresses.
 */
class FixedAddressResolver extends NameResolverProvider {

    /**
     * The list of pre-defined addresses to return.
     */
    final private List<EquivalentAddressGroup> addresses;

    /**
     * The service URI
     */
    private final URI serviceUri;

    /**
     * Creates a new NameResolver.Factory that will always return the given addresses.
     *
     * @param scheme to use in the URI
     * @param addresses the addresses to resolve
     */
    public FixedAddressResolver(String scheme, InetSocketAddress[] addresses) {
        this.serviceUri = URI.create(scheme + addresses[0].getHostString());
        this.addresses = Arrays.stream(addresses)
                .map(EquivalentAddressGroup::new)
                .collect(Collectors.toList());
    }

    public NameResolver newNameResolver(URI ignored, NameResolver.Args args) {

        return new NameResolver() {
            @Override
            public String getServiceAuthority() {
                return serviceUri.getAuthority();
            }

            public void start(Listener2 listener) {
                listener.onResult(ResolutionResult.newBuilder().setAddresses(addresses).setAttributes(Attributes.EMPTY).build());
            }

            public void shutdown() {
            }
        };
    }

    public String getServiceUrl() {
        return serviceUri.toString();
    }

    @Override
    public String getDefaultScheme() {
        return serviceUri.getScheme();
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }
}