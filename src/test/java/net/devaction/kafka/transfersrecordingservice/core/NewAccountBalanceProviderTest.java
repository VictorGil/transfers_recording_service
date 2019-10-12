package net.devaction.kafka.transfersrecordingservice.core;

import java.math.BigDecimal;

import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
class NewAccountBalanceProviderTest {

    private NewAccountBalanceProvider provider = new NewAccountBalanceProvider();

    @Test
    void testProvideNew() {
        BalanceAndVersion inputBV = new BalanceAndVersion(
                new BigDecimal("21.47"), 36L);

        BalanceAndVersion actual = provider.provideNew(inputBV,
                new BigDecimal("7.5"));

        BalanceAndVersion expected = new BalanceAndVersion(
                new BigDecimal("28.97"), 37L);

        Assertions.assertEquals(expected, actual);

        BalanceAndVersion expected2 = new BalanceAndVersion(
                new BigDecimal("28.9700"), 37L);

        Assertions.assertEquals(expected2, actual);
    }
}
