package com.hmy;

/**
 * Utility Class to generate random instances of {@link Customer}
 */
public class CustomerGenerator {

    public CustomerGenerator() {
    }

    private static long counter = 0L;

    public static Customer getNext() {
        final String customer = "customer" + counter++;
        return new Customer(customer, customer + "@gmail.com", 25, "+84 0123 456789" + customer, "male");
    }
}
