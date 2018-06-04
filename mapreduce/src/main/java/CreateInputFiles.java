import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class CreateInputFiles {

    public static void main(String[] args) {
        //number of transactions
        int numberOfTxns = 2000;

        //number of users
        int numberOfUsers = 1000;

        //userIndex
        BigInteger userIndex = new BigInteger("400010002000001");

        generateUsers(numberOfUsers, userIndex);

        generateTransactions(numberOfTxns, 4001, userIndex);
    }
    private static void generateUsers(int numberOfUsers, BigInteger userIndex) {
        File userInputFile = new File("/Users/rj/Documents/GitHub/hadoop/mapreduce/src/main/resources/countTransactions/cardMembers.csv");
        try {
            FileWriter writer = new FileWriter(userInputFile);
            StringBuilder sb = new StringBuilder(numberOfUsers * 30);

            for(int i = 0; i < numberOfUsers; i++) {
                sb.append(userIndex.add(new BigInteger(String.valueOf(i))));
                sb.append(",");
                sb.append("test");
                sb.append(i + 1);
                sb.append("\n");
            }
            writer.write(sb.toString());
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    private static void generateTransactions(int numberOfTxns, int transactionIndex, BigInteger userIndex) {
        File userInputFile = new File("/Users/rj/Documents/GitHub/hadoop/mapreduce/src/main/resources/countTransactions/transactions.csv");
        try {
            FileWriter writer = new FileWriter(userInputFile);
            StringBuilder sb = new StringBuilder(numberOfTxns * 60);

            SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
            GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTime(new Date());
            calendar.add(Calendar.DATE, -2);

            for(int i = 0; i < numberOfTxns; i++) {
                sb.append(transactionIndex + i);
                sb.append(",");
                sb.append(dateFormat.format(new Date()));
                sb.append(",");
                sb.append(dateFormat.format(calendar.getTime()));
                sb.append(",");
                sb.append(String.format("%.2f",generateRandomNumber(1.0, 1000.0)));
                sb.append(",");
                int userOffset = (int) generateRandomNumber(1.0, 1000.0);
                BigInteger user = userIndex.add(new BigInteger(String.valueOf(userOffset)));
                sb.append(user);
                sb.append("\n");
            }
            writer.write(sb.toString());
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    private static double generateRandomNumber(double min, double max) {
        return (Math.random() * ((max - min) + 1) + min);
    }
}
