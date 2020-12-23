import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;

public class CarInputMRGenerator {
    public static String[] CAR_BRANDS = new String[]{
            "BMW",
            "Mercedes-Benz",
            "Toyota",
            "Ford",
            "GM",
            "Honda",
            "Rolls-Royce"
    };

    public static int LEN = CAR_BRANDS.length;

    public static void createTestData() throws IOException {
        File file = new File("output/cars.data");
        Files.deleteIfExists(file.toPath());
        FileWriter writer = new FileWriter(file);

        // write  10 lines
        for (int i = 0; i < 10; i++) {

            // for each line, pick a random number of cars to write
            int j = ThreadLocalRandom.current().nextInt(2, 10);

            for (int k = 0; k <= j; k++) {
                int m = ThreadLocalRandom.current().nextInt(0, LEN);
                writer.write(CAR_BRANDS[m] + " ");
            }
            writer.write("\n");
        }

        writer.close();
    }
}
