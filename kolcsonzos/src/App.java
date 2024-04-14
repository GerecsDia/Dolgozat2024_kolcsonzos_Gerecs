import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class App {
    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("A JDBC driver nem található: " + e.getMessage());
            return;
        }

        System.out.println("----- Kolcsonzok.csv tartalma -----");
        readFile("Kolcsonzok.csv");
        System.out.println("------------------------------------");

        System.out.println("----- Kolcsonzesek.csv tartalma -----");
        readFile("Kolcsonzesek.csv");
        System.out.println("------------------------------------");

        try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/kolcsonzesek_db", "root", "")) {
            System.out.println("Sikeresen kapcsolódva az adatbázishoz!");

            importDataFromCsv("Kolcsonzok.csv", conn, "Kolcsonzok");

            importDataFromCsv("Kolcsonzesek.csv", conn, "Kolcsonzesek");

            System.out.println("Adatok importálása sikeres!");
        } catch (SQLException e) {
            System.err.println("Hiba történt az adatbázishoz való kapcsolódás közben: " + e.getMessage());
        }
    }

    public static void readFile(String filename) {
        try (BufferedReader br = new BufferedReader(new FileReader(filename, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            System.err.println("Hiba történt a(z) " + filename + " beolvasása közben: " + e.getMessage());
        }
    }
    
    public static void importDataFromCsv(String filename, Connection conn, String tableName) {
        try (BufferedReader br = new BufferedReader(new FileReader(filename, StandardCharsets.UTF_8))) {
            String line;
            String insertQuery = "INSERT INTO " + tableName + " (nev, szulIdo) VALUES (?, ?)";
            if (tableName.equals("Kolcsonzesek")) {
                insertQuery = "INSERT INTO " + tableName + " (kolcsonzokId, iro, mufaj, cim) VALUES (?, ?, ?, ?)";
            }
            PreparedStatement pstmt = conn.prepareStatement(insertQuery);    
            
            br.readLine();
    
            while ((line = br.readLine()) != null) {
                String[] data = line.split(";");                 
                for (int i = 0; i < data.length; i++) {
                    pstmt.setString(i + 1, data[i]);
                }                
                pstmt.executeUpdate();
            }
        } catch (IOException | SQLException e) {
            System.err.println("Hiba történt a(z) " + filename + " beolvasása és importálása közben: " + e.getMessage());
        }
    }
}