package com.rtlpowerjson;

import java.util.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Main {

    public static void main(String[] args) throws IOException, ParseException {
	    // write your code here
        String batchID = "20161130-185641";
        String integrationInterval = "10s";
        String dirName = "D:\\Users\\Jackie\\Desktop\\Capstone\\rtl-power-json\\input\\";
        String csvFile = findFile(dirName);

        long startTime = System.currentTimeMillis();
        convert(dirName, csvFile, batchID, integrationInterval);
        long endTime = System.currentTimeMillis();
        double totalTime = (double)((endTime - startTime) / 1000);
        System.out.println("Conversion complete in " + totalTime + " seconds");
    }

    private static String findFile(String dirName) {
        File dir = new File(dirName);
        File[] files = dir.listFiles();
        String csvFileName = "NOTFOUND";

        for (File file : files) {
            if (file.getName().contains("jackie")) {
                csvFileName = dirName + file.getName();
            }
        }
        return csvFileName;
    }

    private static void convert(String dirName, String csvFile, String batchID, String integrationInterval) throws IOException, ParseException {
        if (!csvFile.equals("NOTFOUND")) { // only proceed when there is a valid csv file
            System.out.println("Found .csv file, starting conversion...");
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            Map<String, String> integrationsList = new HashMap<>();
            String line;
            String csvSplitBy = ", ";
            boolean integrationExists;

            //Iterate through entire CSV file
            System.out.println("Iterating through .csv file...");
            while ((line = br.readLine()) != null) {
                String[] entry = line.split(csvSplitBy); //Use comma-space as separator
                integrationExists = false;

                //Convert datetime to unix timestamp
                String datetime = entry[0] + entry[1];
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
                long unixTime =  sdf.parse(datetime).getTime() / 1000;
                String unix = String.valueOf(unixTime);

                //Set boolean if a key already exists in the HashMap
                for (String key : integrationsList.keySet()) {
                    if (key.equals(unix)) {
                        integrationExists = true;
                        break;
                    }
                }

                //If a key already exists in the HashMap
                if (integrationExists) {
                    String json;
                    json = integrationsList.get(unix);
                    int frequencyLow = Integer.parseInt(entry[2]);
                    int frequencyStep = (int)Double.parseDouble(entry[4]);

                    String valuesHeader = "{\"frequencyLow\":\"" + frequencyLow + "\",\"frequencyHigh\":\"" + entry[3] + "\",\"frequencyStep\":\"" + frequencyStep + "\",\"metricValues\":[";
                    String valuesBody = "{\"" + Integer.toString(frequencyLow + (frequencyStep * 0)) + "\":" + entry[6] + "}," +  "{\"" + Integer.toString(frequencyLow + (frequencyStep * 1)) + "\":" + entry[7] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 2)) + "\":" + entry[8] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 3)) + "\":" + entry[9] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 4)) + "\":" + entry[10] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 5)) + "\":" + entry[11] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 6)) + "\":" + entry[12] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 7)) + "\":" + entry[13] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 8)) + "\":" + entry[14] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 9)) + "\":" + entry[15] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 10)) + "\":" + entry[16] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 11)) + "\":" + entry[17] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 12)) + "\":" + entry[18] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 13)) + "\":" + entry[19] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 14)) + "\":" + entry[20] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 15)) + "\":" + entry[21] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 16)) + "\":" + entry[22] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 17)) + "\":" + entry[23] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 18)) + "\":" + entry[24] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 29)) + "\":" + entry[25] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 20)) + "\":" + entry[26] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 21)) + "\":" + entry[27] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 22)) + "\":" + entry[28] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 23)) + "\":" + entry[29] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 24)) + "\":" + entry[30] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 25)) + "\":" + entry[31] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 26)) + "\":" + entry[32] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 27)) + "\":" + entry[33] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 28)) + "\":" + entry[34] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 29)) + "\":" + entry[35] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 30)) + "\":" + entry[36] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 31)) + "\":" + entry[37] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 32)) + "\":" + entry[38] + "}]}";

                    json = json + "," + valuesHeader + valuesBody;
                    integrationsList.put(unix, json);
                }
                //Create a new HashMap entry for this key
                else {
                    int frequencyLow = Integer.parseInt(entry[2]);
                    int frequencyStep = (int)Double.parseDouble(entry[4]);

                    String integrationHeader = "{\"unixTimestamp\":\"" + unixTime + "\",\"date\":\"" + entry[0] + "\",\"time\":\"" + entry[1] + "\",\"totalSamples\":\"" + entry[5] + "\",\"metricSeries\":[";
                    String valuesHeader = "{\"frequencyLow\":\"" + frequencyLow + "\",\"frequencyHigh\":\"" + entry[3] + "\",\"frequencyStep\":\"" + frequencyStep + "\",\"metricValues\":[";
                    String valuesBody = "{\"" + Integer.toString(frequencyLow + (frequencyStep * 0)) + "\":" + entry[6] + "}," +  "{\"" + Integer.toString(frequencyLow + (frequencyStep * 1)) + "\":" + entry[7] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 2)) + "\":" + entry[8] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 3)) + "\":" + entry[9] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 4)) + "\":" + entry[10] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 5)) + "\":" + entry[11] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 6)) + "\":" + entry[12] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 7)) + "\":" + entry[13] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 8)) + "\":" + entry[14] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 9)) + "\":" + entry[15] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 10)) + "\":" + entry[16] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 11)) + "\":" + entry[17] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 12)) + "\":" + entry[18] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 13)) + "\":" + entry[19] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 14)) + "\":" + entry[20] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 15)) + "\":" + entry[21] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 16)) + "\":" + entry[22] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 17)) + "\":" + entry[23] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 18)) + "\":" + entry[24] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 29)) + "\":" + entry[25] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 20)) + "\":" + entry[26] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 21)) + "\":" + entry[27] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 22)) + "\":" + entry[28] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 23)) + "\":" + entry[29] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 24)) + "\":" + entry[30] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 25)) + "\":" + entry[31] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 26)) + "\":" + entry[32] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 27)) + "\":" + entry[33] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 28)) + "\":" + entry[34] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 29)) + "\":" + entry[35] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 30)) + "\":" + entry[36] + "}," + "{\"" + Integer.toString(frequencyLow + (frequencyStep * 31)) + "\":" + entry[37] + "}," +
                            "{\"" + Integer.toString(frequencyLow + (frequencyStep * 32)) + "\":" + entry[38] + "}]}";
                    String integrationJSON = integrationHeader + valuesHeader + valuesBody;

                    integrationsList.put(unix, integrationJSON);
                }
            }

            StringBuilder sb = new StringBuilder();
            SortedSet<String> keys = new TreeSet<>(integrationsList.keySet()); //Sort HashMap by key (unix timestamp)

            //Iterate through HashMap to build JSON
            System.out.println("Building JSON file from HashMap...");
            for (String key : keys) {
                sb.append(integrationsList.get(key)); //Build JSON Body
                sb.append("]},"); //Close off JSON Array and JSON Object
            }

            //Add JSON header fields
            String jsonHeader = "{\"BATCH_ID\":\"" + batchID + "\",\"integrationInterval\":\"" + integrationInterval + "\",\"integrations\":[";
            sb.setLength(sb.length() -1); //Remove final comma as there are no more JSON objects to be added
            sb.append("]}"); //Close off JSON Array and JSON Object

            //Export JSON
            FileWriter fr = new FileWriter(dirName + "/convertedCSV.json");
            fr.write(jsonHeader + sb.toString());

            fr.close();
            br.close();
        }
    }
}
