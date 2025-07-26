package org.sovliv.kafkatask.eratosphen;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 23/07/2025
 */

class EratosphenTest {
    private boolean[] isPrimeNums;
    private Eratosphen eratosphen;
    private final List<Integer> expectedPrimes = Arrays.asList(
            2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97
    );

    @BeforeEach
    void init() {
        eratosphen = new Eratosphen();
        isPrimeNums = eratosphen.isPrimeNums(100);
    }

    @Test
    void test() {
        isPrimeNums = eratosphen.markPrimeNums(isPrimeNums);
        final List<Integer> primeNums = eratosphen.collectPrimeNums(isPrimeNums);
        assertEquals(expectedPrimes, primeNums);
    }
}