package com.ReadingFromDataBase;

import com.sql.databaseconnection.Connection;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;

public class ReadingData {
    private BufferedWriter writer;
    private String Query;
    private java.sql.Connection connection;

    public static void main(String[] args) {
        String zipFLevel=args[0];
        String database= args[1];
        new ReadingData().readingData(zipFLevel,database);
    }
    public void readingData(String fileName, String database){
        try {
            String path="/home/adeelaslam/DataReadingSQL/"+fileName+".csv";
            writer = new BufferedWriter(new FileWriter(new File(path)));
            writer.write("Data, Task, dataSet"+"\n");
           connection = Connection.connetion();
            String table= "select* from "+database;
            java.sql.Statement statement;
            statement = connection.createStatement();
            ResultSet resultSet=statement.executeQuery(table);

            while(resultSet.next()){
                writer.write(resultSet.getString(2)+","+
                        resultSet.getString(3)+","+resultSet.getString(4)+"\n");

            }
            System.out.println("Done");
            connection.close();
            writer.close();
        }
//            String table_selection = "select* from inputparameter";
//            conn = Mysqlconnection.connetion();
//            ResultSet resultset;
//
//            statement = conn.createStatement();
//            resultset = statement.executeQuery(table_selection);
//            while (resultset.next()) {
//        }
        catch (Exception e){
            System.out.println("File not Found"+e);
        }
    }
}
