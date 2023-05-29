package com.hmy;

import JavaSessionize.avro.Customer;

public class CustomerGenerator {
    private static long counter = 0L;

    public static Customer getNext() {
        final String customer = "customer" + counter++;
        return new Customer(customer, customer + "@gmail.com", 25, "+84 0123 456789", "male");
    }
}
