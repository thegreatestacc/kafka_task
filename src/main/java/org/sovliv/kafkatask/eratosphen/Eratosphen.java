package org.sovliv.kafkatask.eratosphen;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 23/07/2025
 */

@Component
public class Eratosphen {

    public boolean[] isPrimeNums(int size) {
        boolean[] primeNums = new boolean[size + 1];
        Arrays.fill(primeNums, true);
        primeNums[0] = false;
        primeNums[1] = false;
        return primeNums;
    }

    public boolean[] markPrimeNums(boolean[] primeNums) {
        int size = primeNums.length - 1;
        for (int num = 2; num * num < size; num++) {
            if (primeNums[num]) {
                for (int j = num * num; j <= size; j += num) {
                    primeNums[j] = false;
                }
            }
        }
        return primeNums;
    }

    public List<Integer> collectPrimeNums(boolean[] isPrimeNums) {
        List<Integer> primeNums = new ArrayList<>();
        for (int i = 2; i < isPrimeNums.length; i++) {
            if (isPrimeNums[i])
                primeNums.add(i);
        }
        return primeNums;
    }
}
