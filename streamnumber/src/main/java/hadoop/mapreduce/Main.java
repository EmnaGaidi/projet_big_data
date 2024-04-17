package hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        String csvFile = "streamnumber/src/main/resources/input/Popular_Spotify_Songs.csv";
        String line = "";
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            int count = 0;
            while ((line = br.readLine()) != null && count < 2) {
                String[] data = line.split(csvSplitBy);
                for (String item : data) {
                    System.out.print(item + " ");
                }
                System.out.println();
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}