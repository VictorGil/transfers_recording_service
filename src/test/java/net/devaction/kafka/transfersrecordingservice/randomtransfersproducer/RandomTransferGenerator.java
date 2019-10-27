package net.devaction.kafka.transfersrecordingservice.randomtransfersproducer;

import java.math.BigDecimal;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.devaction.entity.TransferEntity;

/**
 * @author Víctor Gil
 *
 * since October 2019
 */
public class RandomTransferGenerator {
    private static final Logger log = LoggerFactory.getLogger(RandomTransferGenerator.class);

    private Random random = new Random();

    public TransferEntity generateTransfer() {
        return new TransferEntity(generateAccountId(),
                generateAmount());
    }

    private String generateAccountId() {
        int low = 1;
        int high = 10;

        String auxDigit = "";
        // number between low (inclusive) and high (inclusive)
        int suffix = random.nextInt(high - low + 1) + low;

        if (suffix < 10) {
            auxDigit = "0";
        }
        return "acc-" + auxDigit + suffix;
    }

    private BigDecimal generateAmount() {
        int low = 1;
        int high = 100000;

        // number between low (inclusive) and high (inclusive)
        int cents = random.nextInt(high - low + 1) + low;
        return BigDecimal.valueOf(cents, 2);
    }
}
