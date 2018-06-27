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
import java.util.Set;
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
import com.mongodb.WriteResult;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import com.buyerbuddy.dev.TimeStampUtils;


public class Run {
    final static String characterEncoding = "UTF-8";
    final static String algorithm = "HmacSHA256";
    final static String signatureVersion = "2";
    final static String ordersVersion = "2013-09-01";
    final static String ordersVersionUrl = "Orders/2013-09-01";
    final static String reportsVersion = "2009-01-01";
    final static String reportsVersionUrl = "Reports/2009-01-01";
    final static String awsAccessKey = "AKIAIUT4RSKARFBAFBUQ";
	final static String secretKey = "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";
	final static String serviceUrl = "https://mws.amazonservices.com/";
	
	static String envUri = "mongodb://bbAdmin2018:aA11239066Juniper@ds137720.mlab.com:37720/buyer-buddy";
    static String envDeploy = "buyer-buddy";
    
	static boolean development = true;
	
	public static void main(String[] args) throws Exception {
		
		// Check for development environment
		if ( development == true ) {
			envUri = "mongodb://bbAdminDev:a11239developer@ds237770.mlab.com:37770/buyer-buddy-dev";
			envDeploy = "buyer-buddy-dev";
		}
		
		// Open mongodb connection
		MongoClientURI uri = new MongoClientURI(envUri);
    	MongoClient mongoClient = new MongoClient(uri);
		@SuppressWarnings("deprecation")
		DB db = mongoClient.getDB(envDeploy);
		DBCollection users = db.getCollection("users");
		DBCollection listings = db.getCollection("listings");
		DBCollection salesByWeek = db.getCollection("salesByWeek");
		DBCollection saleByMonth = db.getCollection("salesByMonth");
		
		// Find all active users
		HashMap<String, String> userList = getActiveUsers(users);
		
		// Iterate through each active user
		Set<?> set = userList.entrySet();
	    Iterator<?> iterator = set.iterator();
	    while(iterator.hasNext()) {
			 Map.Entry<?, ?> user = (Map.Entry<?, ?>)iterator.next();
			 String sellerId = user.getKey().toString();
			 String authToken = user.getValue().toString();
			 
			 // Start timer
			 System.out.println("Updating user " + sellerId);
			 long startTime = System.nanoTime();
			 
			 // Call merchant listings report for full item fields
			 String getReportListResponse = signRequest(sellerId, authToken, "GetReportList", null, "full");
			 
			 String reportId = parseReportListResponse(getReportListResponse);
			 
			 String merchantListingsResponse = signRequest(sellerId, authToken, "GetReport", reportId, null);
			 
			 HashMap<String, String> fullListings =  parseMerchantListingsResponse(merchantListingsResponse);
			 //System.out.println("Merchant file listings " + fullListings);
			 //System.out.println("Merchant file listings # " + fullListings.size());
			 
			 // Call up to date inventory list
			 getReportListResponse = signRequest(sellerId, authToken, "GetReportList", null, null);
			 
			 reportId = parseReportListResponse(getReportListResponse);
			 
			 String getInventoryResponse = signRequest(sellerId, authToken, "GetReport", reportId, null);
			 
			 HashMap<String, String> amazonInventoryList = parseInventory(getInventoryResponse, fullListings);
			 //System.out.println("Amazon Inventory " + amazonInventoryList);
			 //System.out.println("Amazon Inventory # " + amazonInventoryList.size());
			 
			 
			 
			 // Call mongodb inventory
			 //HashMap<String, String> mongoInventoryList = getMongoInventory(listings, sellerId);
			 
			 // Check for new listings 
			 /*if (mongoInventoryList.size() != amazonInventoryList.size()) {
				 
				 System.out.println("Updating listings");
				 
				 getReportListResponse = signRequest(sellerId, authToken, "GetReportList", null, "full");
				 
				 reportId = parseReportListResponse(getReportListResponse);
				 
				 String merchantListingsResponse = signRequest(sellerId, authToken, "GetReport", reportId, null);
				 
				 HashMap<String, String> fullListings =  parseMerchantListingsResponse(merchantListingsResponse);
				 System.out.println("Merchant file listings " + fullListings);
				 System.out.println("Merchant file listings # " + fullListings.size());
				 //updateMongoInventory(listings, sellerId, fullListings);
				 
				 //mongoInventoryList = getMongoInventory(listings, sellerId);
			 }*/
			 
			 // Call up to date order list
			 /*String listOrdersResponse = signRequest(sellerId, authToken, "ListOrders", null, null);
			 
			 List<String> amazonOrderIdList = parseListOrdersResponse(listOrdersResponse);
			 
			 HashMap<String, String> ordersToUpdate = new HashMap<String, String>();
			 
			 // check for new orders
			 if(amazonOrderIdList.size() != 0) {
				 
				 for (int i=0; i<amazonOrderIdList.size(); ++i) {
					 String listOrderItemsResponse = signRequest(sellerId, authToken, "ListOrderItems", amazonOrderIdList.get(i), null);
					 
					 List<String> item = parseListOrderItemResponse(listOrderItemsResponse);
					 
					 ordersToUpdate.put(item.get(0),item.get(1));
				 }
				 
			 } else {
				 System.out.println("No new orders");
			 }*/
			 
			 
			 
			 // Calculate change in inventory and push to mongodb
			 
			 // outputs to be used in final math function
			 //System.out.println( "Amazon listings " + amazonInventoryList);
			 //System.out.println( "Amazon listings # " + amazonInventoryList.size());
			 //System.out.println("Mongodb listings " + mongoInventoryList.size());
			 //System.out.println("New orders " + ordersToUpdate.size());
			 
			 
			 // Timer
			 long endTime = System.nanoTime();
			 long duration = ((endTime - startTime)/1000000000);
			 System.out.println(duration + " seconds");
			 
	    }
		// Close mongodb connection
		mongoClient.close();
	}
	
	// Get mongodb active users
	public static HashMap<String, String> getActiveUsers(DBCollection users) throws Exception{
		HashMap<String, String> userList = new HashMap<String, String>();
		
		// Query mongodb for active users
		BasicDBObject query = new BasicDBObject
		   		("status", "Active");
		BasicDBObject fields = new BasicDBObject
			   ("sellerId", 1)
		.append("authToken", 1);
		DBCursor cursor = users.find(query, fields);
		
		while(cursor.hasNext()) {
			
			DBObject user = cursor.next();
			String sellerId = user.get("sellerId").toString();
			String authToken = user.get("authToken").toString();
			
			userList.put(sellerId, authToken);
		}
		return userList;
	}
	
	// Parse GetReportList 
	public static String parseReportListResponse(String reportList) throws Exception {
		
		// Parse xml string
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		InputSource is = new InputSource();
		is.setCharacterStream(new StringReader(reportList));
		Document doc = db.parse(is);
		NodeList nodes = doc.getElementsByTagName("ReportInfo");
		Element element = (Element) nodes.item(0);
		
		NodeList name = element.getElementsByTagName("ReportId");
		Element line = (Element) name.item(0);
		String reportId = getCharacterDataFromElement(line);
		
		return reportId;
	}
	
	// Parse GetReport - inventory
	public static HashMap<String, String> parseInventory(String inventoryResponse, HashMap<String, String> fullListings) throws Exception{
		HashMap<String, String> listingInventory = new HashMap<String, String>();
		
		// Remove headers
		/*inventoryResponse = inventoryResponse.replaceAll("seller-sku\\t","");
    	inventoryResponse = inventoryResponse.replaceAll("fulfillment-channel-sku\\t","");
    	inventoryResponse = inventoryResponse.replaceAll("asin\\t","");
    	inventoryResponse = inventoryResponse.replaceAll("condition-type\\t","");
    	inventoryResponse = inventoryResponse.replaceAll("Warehouse-Condition-code\\t","");
    	inventoryResponse = inventoryResponse.replaceAll("Quantity Available\\t","");*/
    	
    	// Get skus with inventory values attached
    	List<String> list = Arrays.asList(inventoryResponse.split("\\t"));
    	
    	List<String> skus = IntStream.range(0, list.size())
        	    .filter(n -> n % 6 == 0)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	
    	List<String> inventories = IntStream.range(0, list.size())
        	    .filter(n -> n % 6 == 5)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	
    	for (int i=0; i<skus.size(); ++i) {
    		listingInventory.put(skus.get(i), inventories.get(i));
    	}
    	listingInventory.remove("seller-sku");
    	
    	System.out.println(skus.size());
    	System.out.println(inventories.size());
    	System.out.println(listingInventory.size());
    	
    	/*
    	Set<?> set = fullListings.entrySet();
	    Iterator<?> iterator = set.iterator();
	    while(iterator.hasNext()) {
			 Map.Entry<?, ?> listing = (Map.Entry<?, ?>)iterator.next();
			 String activeSku = listing.getKey().toString();
			 
	    }*/
	    
    	// Check for duplicate skus
    	for (int i = 0; i < skus.size(); i++) { 
    		for (int j = i + 1 ; j < skus.size(); j++) { 
    			if (skus.get(i).equals(skus.get(j))) { 
    				System.out.println(skus.get(j));
    			}
    		}
    	}
    	
	    
		return listingInventory;
	}
	
	// Parse ListOrdersResponse
	public static List<String> parseListOrdersResponse(String listOrdersResponse) throws Exception {
		List<String> ordersList = new ArrayList<String>();
		
		// Parse xml string
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	    InputSource is = new InputSource();
	    is.setCharacterStream(new StringReader(listOrdersResponse));
	    Document doc = db.parse(is);
	    NodeList nodes = doc.getElementsByTagName("Order");
	    for (int i = 0; i < nodes.getLength(); i++) {
		  Element element = (Element) nodes.item(i);
		
		  NodeList name = element.getElementsByTagName("AmazonOrderId");
		  Element line = (Element) name.item(0);
		  String idList = getCharacterDataFromElement(line);
		  ordersList.add(idList);
	    }
		return ordersList;
	}
	
	// Parse ListOrderItemResponse
	public static List<String> parseListOrderItemResponse(String listOrderItemResponse) throws Exception{
		List<String> orderItem = new ArrayList<String>();
		
		// Parse xml string
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	    InputSource is = new InputSource();
	    is.setCharacterStream(new StringReader(listOrderItemResponse));
	    Document doc = db.parse(is);
	    NodeList nodes = doc.getElementsByTagName("OrderItem");
	    for (int i = 0; i < nodes.getLength(); i++) {
		  Element element = (Element) nodes.item(i);
		
		  NodeList name = element.getElementsByTagName("ASIN");
		  Element line = (Element) name.item(0);
		  String asin = getCharacterDataFromElement(line);
		  
		  NodeList name2 = element.getElementsByTagName("NumberOfItems");
		  Element line2 = (Element) name2.item(0);
		  String quantity = getCharacterDataFromElement(line2);
		  
		  orderItem.add(asin);
		  orderItem.add(quantity);
	    }
		return orderItem;
	}
	
	// Parse GetMerchantListingsResponse
	public static HashMap<String, String> parseMerchantListingsResponse(String merchantListingsResponse) throws Exception{
		HashMap<String, String> listingValues = new HashMap<String, String>();
		
		
		merchantListingsResponse = merchantListingsResponse.replaceAll( "Migrated Template","");
		merchantListingsResponse = merchantListingsResponse.replaceAll( "merchant-shipping-group","");
		
    	List<String> list = Arrays.asList(merchantListingsResponse.split("\\t"));
    	List<String> asins = IntStream.range(0, list.size())
        	    .filter(n -> n % 39 == 16)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	asins.remove(0);
    	
    	List<String> skus = IntStream.range(0, list.size())
        	    .filter(n -> n % 39 == 3)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	skus.remove(0);
    	
    	List<String> titles = IntStream.range(0, list.size() - 1)
    	    .filter(n -> n % 39 == 0)
    	    .mapToObj(list::get)
    	    .collect(Collectors.toList());
    	titles.remove(0);
    	
    	List<String> prices = IntStream.range(0, list.size())
        	    .filter(n -> n % 39 == 4)
        	    .mapToObj(list::get)
        	    .collect(Collectors.toList());
    	prices.remove(0);
    	
    	for (int i=0; i < skus.size(); ++i) {
    		listingValues.put(skus.get(i), titles.get(i) + "\t" + asins.get(i) + "\t" + prices.get(i));
    	}
		return listingValues;
	}
	
	
	// Get mongodb user inventory
	public static HashMap<String, String> getMongoInventory(DBCollection listings, String sellerId) throws Exception{
		HashMap<String, String> inventoryList = new HashMap<String, String>();
		
		// Query mongodb for user listings
		BasicDBObject query = new BasicDBObject
		   		("sellerId", sellerId);
		DBCursor cursor = listings.find(query);
		
		while(cursor.hasNext()) {
			
			DBObject listing = cursor.next();
			String asin = listing.get("asin").toString();
			String inventory = listing.get("inventory").toString();
			
			inventoryList.put(asin, inventory);
		}
		return inventoryList;
	}
	
	// Update mongodb user listings (for adding new inventory items or initial populating)
	public static void updateMongoInventory(DBCollection listings, String sellerId, HashMap<String, String> fullListings) throws Exception{
		
		// Iterate through each active user
		Set<?> set = fullListings.entrySet();
	    Iterator<?> iterator = set.iterator();
	    while(iterator.hasNext()) {
			 Map.Entry<?, ?> nextListing = (Map.Entry<?, ?>)iterator.next();
			 String asin = nextListing.getKey().toString();
			 String fields = nextListing.getValue().toString();
					 
			// Query mongodb for user listings
			BasicDBObject query = new BasicDBObject
						   ("sellerId", sellerId)
					.append("asin", asin);
			
			DBObject listing = listings.findOne(query);
						
			if (listing == null) {
				
				// parse fields
				List<String> list = Arrays.asList(fields.split("\\t"));

				String price = "";
				if (list.size() == 2) {
					price = "n/a";
				} else {
					price = list.get(2);
				}
			
				BasicDBObject listingObj = new BasicDBObject
						   ("sellerId", sellerId)
					.append("asin", asin)
			        .append("sku", list.get(1))
			        .append("title", list.get(0))
			        .append("price", price)
			        .append("inventory", 0)
			        .append("sales", "")
			        .append("pending", "")
			        .append("salesWeek", "")
			        .append("salesMonth", "")
			        .append("salesQuarter", "")
			        .append("daysOutOfStock", "");
			        
				listings.update(query, listingObj, true, false);
				System.out.println("Created new listing: " + asin);
			}
		}
	}
	
	// Sign requests
	public static String signRequest(String SellerId, String AuthToken, String action, String tokenId, String listType) throws Exception{
		
		// Set version
		String versionUrl = "";
		
		// Get time stamp
		String date = TimeStampUtils.getISO8601StringForCurrentDate();
		
        // Create set of parameters needed and store in a map
        HashMap<String, String> parameters = new HashMap<String,String>();

        // Add required parameters
        parameters.put("AWSAccessKeyId", urlEncode(awsAccessKey));
        parameters.put("SignatureMethod", urlEncode(algorithm));
        parameters.put("SignatureVersion", urlEncode(signatureVersion));
        

        parameters.put("SellerId", urlEncode(SellerId));
        parameters.put("MWSAuthToken", urlEncode(AuthToken));
        parameters.put("Action", urlEncode(action));
        parameters.put("Timestamp", urlEncode(date));
        
        // Add variable parameters
        if( action == "ListOrders") {
        	// All marketplace Id's
        	parameters.put("MarketplaceId.Id.1", urlEncode("ATVPDKIKX0DER"));
        	parameters.put("MarketplaceId.Id.2", urlEncode("A2EUQ1WTGCTBG2"));
        	parameters.put("MarketplaceId.Id.3", urlEncode("A1AM78C64UM0Y8"));
        	parameters.put("MarketplaceId.Id.4", urlEncode("A1RKKUPIHCS9HS"));
        	parameters.put("MarketplaceId.Id.5", urlEncode("A1F83G8C2ARO7P"));
        	parameters.put("MarketplaceId.Id.6", urlEncode("A13V1IB3VIYZZH"));
        	parameters.put("MarketplaceId.Id.7", urlEncode("A1PA6795UKMFR9"));
        	parameters.put("MarketplaceId.Id.8", urlEncode("APJ6JRA9NG5V4"));
        	parameters.put("MarketplaceId.Id.9", urlEncode("A2Q3Y263D00KWC"));
        	parameters.put("MarketplaceId.Id.10", urlEncode("A21TJRUUN4KGV"));
        	parameters.put("MarketplaceId.Id.11", urlEncode("AAHKV2X7AFYLW"));
        	parameters.put("MarketplaceId.Id.12", urlEncode("A1VC38T7YXB528"));
        	parameters.put("MarketplaceId.Id.13", urlEncode("A39IBJ37TRP1C6"));
        	
        	parameters.put("CreatedAfter", urlEncode("2018-06-25T07:00:00Z")); // preset for testing ***** fix this when complete
        	parameters.put("OrderStatus.Status.1", urlEncode("Shipped"));
        	parameters.put("Version", urlEncode(ordersVersion));
        	versionUrl = ordersVersionUrl;
        } 
        if (action == "ListOrderItems") {
        	parameters.put("AmazonOrderId", urlEncode(tokenId));
        	parameters.put("Version", urlEncode(ordersVersion));
        	versionUrl = ordersVersionUrl;
        }
        if (action == "GetReportList") {
        	if ( listType == "full") {
        		parameters.put("ReportTypeList.Type.1", urlEncode("_GET_MERCHANT_LISTINGS_DATA_"));
            	parameters.put("Version", urlEncode(reportsVersion));
            	versionUrl = reportsVersionUrl;
        	} else {
        		parameters.put("ReportTypeList.Type.1", urlEncode("_GET_AFN_INVENTORY_DATA_"));
            	parameters.put("Version", urlEncode(reportsVersion));
            	versionUrl = reportsVersionUrl;
        	}
        }
        if (action == "GetReport") {
        	parameters.put("ReportId", urlEncode(tokenId));
        	parameters.put("Version", urlEncode(reportsVersion));
        	versionUrl = reportsVersionUrl;
        }
        
        // Format the parameters as they will appear in final format
        String formattedParameters = calculateStringToSignV2(parameters, serviceUrl, versionUrl);
        String signature = sign(formattedParameters, secretKey);

        // Add signature to the parameters
        parameters.put("Signature", urlEncode(signature));
        
        // Create URL
        String https_url = calculateStringToSignV2(parameters, serviceUrl, versionUrl);
        https_url = https_url.substring(28);
        https_url = serviceUrl + https_url;
        https_url = https_url.replaceAll("\n","");
        https_url = https_url.replaceAll(versionUrl, versionUrl + "?");
        URL url = new URL(https_url);
	    HttpsURLConnection con = (HttpsURLConnection)url.openConnection();
	    
	    // Get response
	    String response = new String();
		BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
		if (action == "GetReport") {
			for (String line; (line = br.readLine()) != null; response += line + "\t");
		} else {
			for (String line; (line = br.readLine()) != null; response += line);
		}
		br.close();
		//System.out.println(response);
    	return response;
	}
    
	// Amazon signing functions 
    public static String getCharacterDataFromElement(Element e) {
        Node child = e.getFirstChild();
        if (child instanceof CharacterData) {
          CharacterData cd = (CharacterData) child;
          return cd.getData();
        }
        return "";
      }

    public static String calculateStringToSignV2(
        Map<String, String> parameters, String serviceUrl, String versionUrl)
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
        data.append("\n/" + versionUrl);
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

    private static String sign(String data, String secretKey)
            throws NoSuchAlgorithmException, InvalidKeyException,
                   IllegalStateException, UnsupportedEncodingException {
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(secretKey.getBytes(characterEncoding),
            algorithm));
        byte[] signature = mac.doFinal(data.getBytes(characterEncoding));
        String signatureBase64 = new String(Base64.encodeBase64(signature),
        		characterEncoding);
        return new String(signatureBase64);
    }

    private static String urlEncode(String rawValue) {
        String value = (rawValue == null) ? "" : rawValue;
        String encoded = null;

        try {
            encoded = URLEncoder.encode(value, characterEncoding)
                .replace("+", "%20")
                .replace("*", "%2A")
                .replace("%7E","~");
        } catch (UnsupportedEncodingException e) {
            System.err.println("Unknown encoding: " + characterEncoding);
            e.printStackTrace();
        }

        return encoded;
    }
  }
