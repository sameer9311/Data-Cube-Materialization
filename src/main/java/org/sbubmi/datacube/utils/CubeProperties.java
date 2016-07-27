package org.sbubmi.datacube.utils;

import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

public class CubeProperties {
	
	private Map<String, String> dimensionMap;
	private String measure;
	private String fact;
	
	/**  
	 * 
	 * @param dimensionMap: the key is the column name in db and value is the name of that dimension displayed in cube;
	 * 					   Map will also prevent duplicate values of columns 
	 * @param measure: the measure which will be applied on the cube
	 * @param fact: the fact of the cube 
	 */
		
	public CubeProperties(Map<String, String> dimensionMap, String measure, String fact) {
		super();
		this.dimensionMap = dimensionMap;
		this.measure = measure;
		this.fact = fact;
	}	
	
	/**
	 * Reads the cube dimensions and measures to be applied on the cube from XML file
	 * 
	 * @return CubeProperties object having the dimensionMap and measure fields
	 */
	
	public static CubeProperties readCubeProperties()
	{
		CubeProperties cubePropertyObject = null;
				
		try {

			File fXmlFile = new File("/home/Data-Cube-Materialization/src/main/resources/cubeproperties.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
					
			//optional, but recommended
			//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			doc.getDocumentElement().normalize();

			System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
					
			NodeList dimensionList = doc.getElementsByTagName("dimension");
			
			String dbColumnName;
			String cubeDimName;
			Map<String, String> dimensionMap = new HashMap<String, String>();

			for (int temp = 0; temp < dimensionList.getLength(); temp++) {

				Node dimensionNode = dimensionList.item(temp);
						
				System.out.println("\nCurrent Element :" + dimensionNode.getNodeName());
						
				if (dimensionNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) dimensionNode;
					
					dbColumnName = eElement.getElementsByTagName("columnname").item(0).getTextContent();
					cubeDimName = eElement.getElementsByTagName("name").item(0).getTextContent();
					
					dimensionMap.put(dbColumnName, cubeDimName);				

				}
			}
			
			String measure = doc.getElementsByTagName("measure").item(0).getTextContent();
			String fact = doc.getElementsByTagName("fact").item(0).getTextContent();
			
			cubePropertyObject = new CubeProperties(dimensionMap, measure, fact);			
			
		    } catch (Exception e) {
			e.printStackTrace();
		    }
		return cubePropertyObject;
	}

	public Map<String, String> getDimensionMap() {
		return dimensionMap;
	}

	public String getMeasure() {
		return measure;
	}

	public String getFact() {
		return fact;
	}

	
	
	
	
	
	

}
