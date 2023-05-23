package com.hmy;

import java.util.Objects;

public class Customer {
    private String name;
    private String email;
    private int age;
    private String phoneNumber;
    private String gender;

    public Customer(String name, String email, int age, String phoneNumber, String gender) {
        this.name = name;
        this.email = email;
        this.age = age;
        this.phoneNumber = phoneNumber;
        this.gender = gender;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Customer customer = (Customer) o;
        return Objects.equals(name, customer.name) && Objects.equals(email, customer.email) && Objects.equals(age, customer.age) && Objects.equals(phoneNumber, customer.phoneNumber) && Objects.equals(gender, customer.gender);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, email, age, phoneNumber, gender);
    }

    @Override
    public String toString() {
        return String.format("[ Name: %s | Email Address: %s | Age: %s | Phone: %s | Gender: %s ]", name, email, age, phoneNumber, gender);
    }
}
