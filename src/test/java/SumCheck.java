

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * 
 * @author sameer
 *
 * This class check whether the sum produced by mapreduce is correct
 */

public class SumCheck {

	public static void main(String[] args) {

		try (BufferedReader br = new BufferedReader(new FileReader("queryResults.txt")))
		{

			String sCurrentLine;
			int sum = 0;

			while ((sCurrentLine = br.readLine()) != null) {
				String[] a = sCurrentLine.split(",");
				sum += Integer.parseInt(a[1]);
			}
			System.out.println(sum);

		} catch (IOException e) {
			e.printStackTrace();
		} 

	}
}
