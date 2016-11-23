package com.rtlpowerjson;

import java.util.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Main {

    public static void main(String[] args) throws IOException, ParseException {
	    // write your code here
        String batchID = "20161120-171300";
        String integrationInterval = "30m";
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
            String csvSplitBy = ",";
            boolean integrationExists;

            //Iterate through entire CSV file
            System.out.println("Iterating through .csv file...");
            while ((line = br.readLine()) != null) {
                String[] entry = line.split(csvSplitBy); //Use comma as separator
                String time = entry[1].substring(1); //rtl_power adds stupid linebreak before time
                integrationExists = false;

                //Convert datetime to unix timestamp
                String datetime = entry[0] + time;
                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyyHH:mm:ss");
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

                    String valuesHeader = "{\"frequencyLow\":\"" + entry[2] + "\",\"frequencyHigh\":\"" + entry[3] + "\",\"frequencyStep\":\"" + entry[4] + "\",\"metricValues\":[";
                    String valuesBody = entry[6] + "," +  entry[7] + "," + entry[8] + "," + entry[9] + "," + entry[10] + "," + entry[11] + "," + entry[12] + "," + entry[13] + "," + entry[14] +
                            "," + entry[15] + "," + entry[16] + "," + entry[17] + "," + entry[18] + "," + entry[19] + "," + entry[20] + "," + entry[21] + "," + entry[22] + "," + entry[23] + "," +
                            entry[24] + "," + entry[25] + "," + entry[26] + "," + entry[27] + "," + entry[28] + "," + entry[29] + "," + entry[30] + "," + entry[31] + "," + entry[32] + "," + entry[33] +
                            "," + entry[34] + "," + entry[35] + "," + entry[36] + "," + entry[37] + "," + entry[38] + "]}";

                    json = json + "," + valuesHeader + valuesBody;
                    integrationsList.put(unix, json);
                }
                //Create a new HashMap entry for this key
                else {
                    String integrationHeader = "{\"unix\":\"" + unix + "\",\"date\":\"" + entry[0] + "\",\"time\":\"" + time + "\",\"totalSamples\":\"" + entry[5] + "\",\"metricSeries\":[";
                    String valuesHeader = "{\"frequencyLow\":\"" + entry[2] + "\",\"frequencyHigh\":\"" + entry[3] + "\",\"frequencyStep\":\"" + entry[4] + "\",\"metricValues\":[";
                    String valuesBody = entry[6] + "," +  entry[7] + "," + entry[8] + "," + entry[9] + "," + entry[10] + "," + entry[11] + "," + entry[12] + "," + entry[13] + "," + entry[14] +
                            "," + entry[15] + "," + entry[16] + "," + entry[17] + "," + entry[18] + "," + entry[19] + "," + entry[20] + "," + entry[21] + "," + entry[22] + "," + entry[23] + "," +
                            entry[24] + "," + entry[25] + "," + entry[26] + "," + entry[27] + "," + entry[28] + "," + entry[29] + "," + entry[30] + "," + entry[31] + "," + entry[32] + "," + entry[33] +
                            "," + entry[34] + "," + entry[35] + "," + entry[36] + "," + entry[37] + "," + entry[38] + "]}";
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
