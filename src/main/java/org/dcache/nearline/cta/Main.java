package org.dcache.nearline.cta;

public class Main {

    public static void main(String[] args) {

        CtaNearlineStorageProvider provider = new CtaNearlineStorageProvider();
        CtaNearlineStorage driver = provider.createNearlineStorage("cta", "cta");

        System.out.println(provider.getDescription());
    }
}
