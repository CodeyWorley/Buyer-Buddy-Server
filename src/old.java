package com.buyerbuddy.dev;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.net.URL;
import java.io.*;
import java.io.StringReader;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HttpsURLConnection;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.codec.binary.Base64;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Node;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.buyerbuddy.dev.TimeStampUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class Run {
    private static final String CHARACTER_ENCODING = "UTF-8";
    final static String ALGORITHM = "HmacSHA256";
    final static String SERVICE_URL = "https://mws.amazonservices.com/";
    final static String SIGNATURE_VERSION = "2";
    final static String VERSION = "2009-01-01";
    final static String LISTING_DATA = "_GET_MERCHANT_LISTINGS_DATA_";
    final static String INVENTORY_DATA = "_GET_AFN_INVENTORY_DATA_";
    
    final static String MONGO_URI_DEV = "mongodb://bbAdminDev:a11239developer@ds237770.mlab.com:37770/buyer-buddy-dev";
    final static String DEPLOY_DEV = "buyer-buddy-dev";
    final static String MONGO_URI_PROD = "mongodb://bbAdmin2018:aA11239066Juniper@ds137720.mlab.com:37720/buyer-buddy";
    final static String DEPLOY_PROD = "buyer-buddy";
    
	final static String ENV_URI = MONGO_URI_DEV;
	final static String ENV_DEPLOY = DEPLOY_DEV;

	final static String AWS_ACCESS_KEY = "AKIAIUT4RSKARFBAFBUQ";
	final static String SECRET_KEY = "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";
	
	static Calendar calendar = Calendar.getInstance();
	static String dayOfYear = Integer.toString(calendar.get(Calendar.DAY_OF_YEAR));
	
	
	public static void main(String[] args) throws Exception {
		
		List<String> activeUsers = getActiveUsers();
		
		for (int i = 0; i < activeUsers.size(); i++) {
		   
			String user = activeUsers.get(i);
			
			List<String> fields = Arrays.asList(user.split("\\t"));
			
			// Set Buyer Variables
	    	String SellerId = fields.get(0);
	    	String AuthToken = fields.get(1);
	        String date = TimeStampUtils.getISO8601StringForCurrentDate();
	        
	        // Get Report Id's
	    	String listing = getReportList(SellerId, AuthToken, date, LISTING_DATA);
	    	String inventory = getReportList(SellerId, AuthToken, date, INVENTORY_DATA);
	    	
	    	// Get Data Report
	    	String listingsToParse = getReport(SellerId, AuthToken, date, listing);
	    	
	    	// Get Inventory Report
	    	String inventoryToParse = getReport(SellerId, AuthToken, date, inventory);
	    	
	    	// Parse Response
	    	List<String> fullItems = parseReport(listingsToParse);
	    	List<String> inventoryItems = parseInventory(inventoryToParse);
	    	
	    	//Push to Mongo
	    	pushToMongo(fullItems, inventoryItems, date, SellerId);
			
		}
		
	}

    public static void pushToMongo(List<String> fullItems, List<String> inventoryItems, String date, String sellerId) throws Exception{
    	
    	MongoClientURI uri = new MongoClientURI(ENV_URI);
    	MongoClient mongoClient = new MongoClient(uri);
		@SuppressWarnings("deprecation")
		DB db = mongoClient.getDB(ENV_DEPLOY);
		
		DBCollection coll = db.getCollection("listings");
		
		boolean fixInventory = false;
		
		System.out.println("Checking for new Listings");
		for (int i=0; i < fullItems.size(); ++i) {
			String item = fullItems.get(i);
			List<String> list = Arrays.asList(item.split("\\t"));
			
			BasicDBObject query = new BasicDBObject
			   		("asin", list.get(2));
			
			DBObject listing = coll.findOne(query);
			
			
			if (listing == null) {
				System.out.println("Creating new Listing " + list.get(2));
				
				fixInventory = true;
				
				BasicDBObject days = new BasicDBObject("1", 0);
				for (int x=2; x < 366; ++x) {
					String day = String.valueOf(x);
					days.append(day, 0);
				}
				
				BasicDBObject doc = new BasicDBObject
							   ("title", list.get(0))
				        .append("sku", list.get(1))
				        .append("asin", list.get(2))
				        .append("price", list.get(3))
				        .append("inventory", 0)
				        .append("sellerId", sellerId)
				        .append("2018", days);
				
				coll.update(query, doc, true, false);
				
			}
		}
		if (fixInventory == true) {
			// fix inventory before selling function
			System.out.println("Updating New Listing Inventory");
			
			for (int i=0; i < inventoryItems.size(); ++i) {
				String item = inventoryItems.get(i);
				List<String> list = Arrays.asList(item.split("\\t"));
				
				BasicDBObject query = new BasicDBObject
						   		("asin", list.get(0));
				
				int newInventory = Integer.parseInt(list.get(1));
				
				BasicDBObject doc = new BasicDBObject("inventory", newInventory);
				BasicDBObject set = new BasicDBObject("$set", doc);
				coll.update(query, set);
			}
		}
		
		System.out.println("Updating Inventory");
		for (int i=0; i < inventoryItems.size(); ++i) {
			String item = inventoryItems.get(i);
			List<String> list = Arrays.asList(item.split("\\t"));
			
			BasicDBObject query = new BasicDBObject
					   		("asin", list.get(0));
			
			DBObject listing = coll.findOne(query);
			
			if (listing != null) {
				int inventory = Integer.parseInt(listing.get("inventory").toString());
				int newInventory = Integer.parseInt(list.get(1));
				
				if (inventory < newInventory) {
					BasicDBObject doc = new BasicDBObject("inventory", newInventory);
					BasicDBObject set = new BasicDBObject("$set", doc);
					coll.update(query, set);
				}
				if (inventory > newInventory) {
					int sold = inventory - newInventory;
					System.out.println("Listing " + list.get(0) + " sold " + sold + " item/s");
					System.out.println(inventory + " -> " + newInventory);
					
					BasicDBObject day = new BasicDBObject(dayOfYear, sold);
					BasicDBObject doc = new BasicDBObject
							("inventory", newInventory)
					 .append("2018", day);
					
					BasicDBObject set = new BasicDBObject("$set", doc);
					coll.update(query, set);
				} 
			}
		}
		
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy  h:mm a");
		Date newDate = new Date();
		String updatedDate = dateFormat.format(newDate);
		//String day = "2018." + dayOfYear.toString();
		BasicDBObject update = new BasicDBObject
					   ("name", "lastUpdate")
		        .append("updated", updatedDate)
		        .append("day", dayOfYear);
				   BasicDBObject queryUpdate = new BasicDBObject
					   ("name", "lastUpdate");
		coll.update(queryUpdate, update, true, false);
		
		mongoClient.close();
    }
	
	public static List<String> getActiveUsers() throws Exception{
		List<String> activeUsers = new ArrayList<String>();
		
		MongoClientURI uri = new MongoClientURI(ENV_URI);
    	MongoClient mongoClient = new MongoClient(uri);
		@SuppressWarnings("deprecation")
		DB db = mongoClient.getDB(ENV_DEPLOY);
		
		DBCollection coll = db.getCollection("users");
		
		BasicDBObject query = new BasicDBObject
				   		("status", "Active");
		BasicDBObject fields = new BasicDBObject
				   ("sellerId", 1)
	        .append("authToken", 1);
		
		DBCursor cursor = coll.find(query, fields);
		
		while(cursor.hasNext()) {
			String field = cursor.next().toString();
			
			// Parse string
			field = field.replaceAll(" : ","");
			field = field.replaceAll("\"","");
			field = field.replaceAll(" ","");
			field = field.replaceAll("}","");
			field = field.replaceAll("sellerId","");
			field = field.replaceAll("authToken","");
			
			List<String> list = Arrays.asList(field.split(","));
			
			String id = list.get(1) + "\t" + list.get(2);
			
		    activeUsers.add(id);
		}
		
		mongoClient.close();
		return activeUsers;
	}
    
    public static List<String> parseInventory(String toParse) throws Exception{
    	
    	List<String> inventoryItems = new ArrayList<String>();
    	List<String> inventory = new ArrayList<String>();
    	
    	toParse = toParse.replaceAll("seller-sku\\t","");
    	toParse = toParse.replaceAll("fulfillment-channel-sku\\t","");
    	toParse = toParse.replaceAll("asin\\t","");
    	toParse = toParse.replaceAll("condition-type\\t","");
    	toParse = toParse.replaceAll("Warehouse-Condition-code\\t","");
    	toParse = toParse.replaceAll("Quantity Available","");
    	List<String> list = Arrays.asList(toParse.split("\\t"));
    	
    	List<String> skus = IntStream.range(0, list.size())
        	    .filter(n -> n % 5 == 0)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	
    	List<String> asins = IntStream.range(0, list.size())
        	    .filter(n -> n % 5 == 2)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	
    	for (int i=0; i < skus.size(); ++i) {
    		String s = skus.get(i);
        	
        	Matcher matcher = Pattern.compile("\\d+").matcher(s);
        	matcher.find();
        	int inv = Integer.valueOf(matcher.group());
        	
        	inventory.add(Integer.toString(inv));
    	}
    	
    	inventory.remove(0);
    	for (int i=0; i < asins.size(); ++i) {
    		inventoryItems.add(asins.get(i) + "\t" + inventory.get(i));
    	}
    	
    	return inventoryItems;
    }
    
    public static List<String> parseReport(String toParse) throws Exception{
    	
    	List<String> fullItems = new ArrayList<String>();
    	
    	toParse = toParse.replaceAll("merchant-shipping-group","");
    	toParse = toParse.replaceAll("Migrated Template","");
    	List<String> list = Arrays.asList(toParse.split("\\t"));
    	
    	List<String> titles = IntStream.range(0, list.size() - 1)
    	    .filter(n -> n % 39 == 0)
    	    .mapToObj(list::get)
    	    .collect(Collectors.toList());
    	
    	List<String> skus = IntStream.range(0, list.size())
        	    .filter(n -> n % 39 == 3)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	
    	List<String> asins = IntStream.range(0, list.size())
        	    .filter(n -> n % 39 == 16)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	
    	List<String> prices = IntStream.range(0, list.size())
        	    .filter(n -> n % 39 == 4)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	
    	for (int i=0; i < titles.size(); ++i) {
    		fullItems.add(titles.get(i) + "\t" + skus.get(i) + "\t" + asins.get(i) + "\t" + prices.get(i));
    	}
    	fullItems.remove(0);
    	
    	return fullItems;
    }
    
    public static String getReportList(String SellerId, String AuthToken, String date, String type) throws Exception {
    	// Set variable parameters
        String action = "GetReportList";

        // Create set of parameters needed and store in a map
        HashMap<String, String> parameters = new HashMap<String,String>();

        // Add required parameters
        parameters.put("AWSAccessKeyId", urlEncode(AWS_ACCESS_KEY));
        parameters.put("SellerId", urlEncode(SellerId));
        parameters.put("MWSAuthToken", urlEncode(AuthToken));
        parameters.put("SignatureMethod", urlEncode(ALGORITHM));
        parameters.put("SignatureVersion", urlEncode(SIGNATURE_VERSION));
        parameters.put("Timestamp", urlEncode(date));
        parameters.put("Version", urlEncode(VERSION));
        parameters.put("Action", urlEncode(action));
        parameters.put("ReportTypeList.Type.1", urlEncode(type));
        parameters.put("MaxCount", urlEncode("1"));
        
        // Format the parameters as they will appear in final format (without the signature parameter)
        String formattedParameters = calculateStringToSignV2(parameters, SERVICE_URL);
        String signature = sign(formattedParameters, SECRET_KEY);

        // Add signature to the parameters
        parameters.put("Signature", urlEncode(signature));
        
        // Create URL
        String https_url = calculateStringToSignV2(parameters, SERVICE_URL);
        https_url = https_url.substring(28);
        https_url = SERVICE_URL + https_url;
        https_url = https_url.replaceAll("\n","");
        https_url = https_url.replaceAll("Reports/2009-01-01","Reports/2009-01-01?");
        URL url = new URL(https_url);
	    HttpsURLConnection con = (HttpsURLConnection)url.openConnection();
	    
	    // Get response
	    String response = new String();
		BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
		for (String line; (line = br.readLine()) != null; response += line);
		br.close();
		
		// Parse xml string
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	    InputSource is = new InputSource();
	    is.setCharacterStream(new StringReader(response));
	    Document doc = db.parse(is);
	    NodeList nodes = doc.getElementsByTagName("ReportInfo");
	    String idList = "";
	    for (int i = 0; i < nodes.getLength(); i++) {
		  Element element = (Element) nodes.item(i);
		
		  NodeList name = element.getElementsByTagName("ReportId");
		  Element line = (Element) name.item(0);
		  idList = getCharacterDataFromElement(line);
	    }
    	return idList;
    }
    
	public static String getReport(String SellerId,String AuthToken, String date, String id) throws Exception{
		// Set variable parameters
        String action = "GetReport";

        // Create set of parameters needed and store in a map
        HashMap<String, String> parameters = new HashMap<String,String>();

        // Add required parameters
        parameters.put("AWSAccessKeyId", urlEncode(AWS_ACCESS_KEY));
        parameters.put("SellerId", urlEncode(SellerId));
        parameters.put("MWSAuthToken", urlEncode(AuthToken));
        parameters.put("SignatureMethod", urlEncode(ALGORITHM));
        parameters.put("SignatureVersion", urlEncode(SIGNATURE_VERSION));
        parameters.put("Timestamp", urlEncode(date));
        parameters.put("Version", urlEncode(VERSION));
        parameters.put("Action", urlEncode(action));
        parameters.put("ReportId", urlEncode(id));
        
        // Format the parameters as they will appear in final format (without the signature parameter)
        String formattedParameters = calculateStringToSignV2(parameters, SERVICE_URL);
        String signature = sign(formattedParameters, SECRET_KEY);

        // Add signature to the parameters
        parameters.put("Signature", urlEncode(signature));
        
        // Create URL
        String https_url = calculateStringToSignV2(parameters, SERVICE_URL);
        https_url = https_url.substring(28);
        https_url = SERVICE_URL + https_url;
        https_url = https_url.replaceAll("\n","");
        https_url = https_url.replaceAll("Reports/2009-01-01","Reports/2009-01-01?");
        URL url = new URL(https_url);
	    HttpsURLConnection con = (HttpsURLConnection)url.openConnection();
	    
	    // Get response
	    String response = new String();
		BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
		for (String line; (line = br.readLine()) != null; response += line);
		br.close();
		
    	return response;
	}
    
    public static String getCharacterDataFromElement(Element e) {
        Node child = e.getFirstChild();
        if (child instanceof CharacterData) {
          CharacterData cd = (CharacterData) child;
          return cd.getData();
        }
        return "";
      }

    public static String calculateStringToSignV2(
        Map<String, String> parameters, String serviceUrl)
            throws SignatureException, URISyntaxException {
        // Sort the parameters alphabetically by storing
        // in TreeMap structure
        Map<String, String> sorted = new TreeMap<String, String>();
        sorted.putAll(parameters);

        // Set endpoint value
        URI endpoint = new URI(serviceUrl.toLowerCase());

        // Create flattened (String) representation
        StringBuilder data = new StringBuilder();
        data.append("GET\n");
        data.append(endpoint.getHost());
        data.append("\n/Reports/2009-01-01");
        data.append("\n");

        Iterator<Entry<String, String>> pairs =
          sorted.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            if (pair.getValue() != null) {
                data.append( pair.getKey() + "=" + pair.getValue());
            }
            else {
                data.append( pair.getKey() + "=");
            }

            // Delimit parameters with ampersand (&)
            if (pairs.hasNext()) {
                data.append( "&");
            }
        }
        return data.toString();
    } 

    // Sign the text with the given secret key and convert to base64
    private static String sign(String data, String secretKey)
            throws NoSuchAlgorithmException, InvalidKeyException,
                   IllegalStateException, UnsupportedEncodingException {
        Mac mac = Mac.getInstance(ALGORITHM);
        mac.init(new SecretKeySpec(secretKey.getBytes(CHARACTER_ENCODING),
            ALGORITHM));
        byte[] signature = mac.doFinal(data.getBytes(CHARACTER_ENCODING));
        String signatureBase64 = new String(Base64.encodeBase64(signature),
            CHARACTER_ENCODING);
        return new String(signatureBase64);
    }

    private static String urlEncode(String rawValue) {
        String value = (rawValue == null) ? "" : rawValue;
        String encoded = null;

        try {
            encoded = URLEncoder.encode(value, CHARACTER_ENCODING)
                .replace("+", "%20")
                .replace("*", "%2A")
                .replace("%7E","~");
        } catch (UnsupportedEncodingException e) {
            System.err.println("Unknown encoding: " + CHARACTER_ENCODING);
            e.printStackTrace();
        }

        return encoded;
    }
  }
