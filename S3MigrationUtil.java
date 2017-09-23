package com.test.aws;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class S3MigrationUtil {

	private static final Logger logger = LoggerFactory.getLogger(S3MigrationUtil.class);
	private static final int START_COUNT = 2450;
	private static final int END_COUNT = 6000;

	private String companyCode = "contenthub";
	private boolean useProxy 			= false;
	private String proxyHost 			= "PROXY";
	private int proxyPort 	 			= 8080;
	
	private String sourceHostName 			= "s3-ap-southeast-1.amazonaws.com";
	private String sourceAccessKey 			= "ACCESS_KEY_SOURCE";
	private String sourceSecretKey			= "SECRET_KEY_SOURCE";
	private String sourceBucketName 	= "sourcebucket";
	private String sourceEndPoint 		= sourceHostName + "/" + sourceBucketName;
	private Regions sourceRegion = Regions.AP_SOUTHEAST_1;
	
	private String targetHostName 			= "s3-ap-southeast-1.amazonaws.com";
	private String targetAccessKey 		= "ACCESS_KEY_TARGET";
	private String targetSecretKey		= "SECRET_KEY_TARGET";
	private String targetBucketName 	= "targetBucket";
	private String targetEndPoint 		= targetHostName + "/" + targetBucketName;
	private Regions targetRegion = Regions.AP_SOUTHEAST_1;

	private static File tempDir = new File("temp_s3_dir");
	private File successFile = new File(tempDir,"success.txt");
	private File errorFile = new File(tempDir,"error.txt");

	public static void main(String[] args) throws IOException {
		tempDir.mkdirs();
		S3MigrationUtil s3 = new S3MigrationUtil();
		s3.migrate();
		logger.info("Migration to S3 Finished.");
	}

	public void migrate() throws IOException {
   		AWSCredentials credentials 	= null;
   		try {            
   			credentials = new BasicAWSCredentials(sourceAccessKey, sourceSecretKey);
   		} catch (Exception e) {
   			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. " +
   				"Please make sure that your credentials file is at the correct ", e);
   		}
       
   		ClientConfiguration clientConfiguration = new ClientConfiguration();
   		clientConfiguration.setSocketTimeout(200000);
   		clientConfiguration.setProtocol(Protocol.HTTP);
       
   		if (useProxy) {
   			clientConfiguration.setProxyHost(proxyHost);
   			clientConfiguration.setProxyPort(proxyPort);
   		} else{
   			logger.debug("credentials Access KeyId= " + credentials.getAWSAccessKeyId());
   			logger.debug("credentials Access Secret Key= " + credentials.getAWSSecretKey());
   		}
   		
   		logger.debug("Source endPoint= " + sourceEndPoint);              
       
   		AmazonS3 sourceS3Conn = new AmazonS3Client(credentials,clientConfiguration);       
   		sourceS3Conn.setRegion(Region.getRegion(sourceRegion));
   		sourceS3Conn.setEndpoint(sourceEndPoint);

   		logger.debug("===========================================");
   		logger.debug("Getting Started with Amazon S3");
   		logger.debug("===========================================\n");

   		try {
   			final List<String> keyFilters = new ArrayList<>();
   			keyFilters.add("AssetFiles/");
   			migrateSourceBucketContent(sourceS3Conn, sourceBucketName, keyFilters);
   		} catch (AmazonServiceException ase) {
   			logger.debug("Caught an AmazonServiceException, which means your request made it "
   				+ "to Amazon S3, but was rejected with an error response for some reason.");
           logger.debug("Error Message:    " + ase.getMessage());
           logger.debug("HTTP Status Code: " + ase.getStatusCode());
           logger.debug("AWS Error Code:   " + ase.getErrorCode());
           logger.debug("Error Type:       " + ase.getErrorType());
           logger.debug("Request ID:       " + ase.getRequestId());
   		} catch (AmazonClientException ace) {
           logger.debug("Caught an AmazonClientException, which means the client encountered "
        		+ "a serious internal problem while trying to communicate with S3, "
        		+ "such as not being able to access the network.");
           logger.debug("Error Message: " + ace.getMessage());
   		} finally{
   		}
	}

	private void migrateSourceBucketContent(AmazonS3 s3Conn, String bucketName, List<String> keyFilters) {

   		AWSCredentials credentials 	= null;
   		try {            
   			credentials = new BasicAWSCredentials(targetAccessKey, targetSecretKey);
   		} catch (Exception e) {
   			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. " +
   				"Please make sure that your credentials file is at the correct ", e);
   		}
       
   		ClientConfiguration clientConfiguration = new ClientConfiguration();
   		clientConfiguration.setSocketTimeout(200000);
   		clientConfiguration.setProtocol(Protocol.HTTP);
       
   		if (useProxy) {
   			clientConfiguration.setProxyHost(proxyHost);
   			clientConfiguration.setProxyPort(proxyPort);
   		}
   		            
       
   		AmazonS3 targetConn = new AmazonS3Client(credentials,clientConfiguration);       
   		targetConn.setRegion(Region.getRegion(targetRegion));
   		targetConn.setEndpoint(targetEndPoint);

		try {
			int cnt = 0;
			for (String keyFilter : keyFilters) {
				ListObjectsRequest listRequest = new ListObjectsRequest();
				listRequest.withBucketName(bucketName);
				listRequest.withPrefix(keyFilter);
			
			
				ObjectListing listing;
				do {
					listing = s3Conn.listObjects(listRequest);
					logger.info("Starting migrating from bucket=" + listing.getBucketName() + " with prefix=" + listing.getPrefix() + " size=" + listing.getObjectSummaries().size());
					for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
						cnt++;
						
						if (cnt > START_COUNT) {
							final String key = objectSummary.getKey();
							final S3Object object = s3Conn.getObject(new GetObjectRequest(bucketName, key));
							logger.info(cnt + " " + key);
							migrateSourceContent(targetConn, object, key);
						}

						if (cnt == END_COUNT) {
							logger.info("Stopping because it exceeded the COUNT_LIMIT");
							return;
						}
					}
					listRequest.setMarker(listing.getNextMarker());
				} while (listing.isTruncated());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void migrateSourceContent(AmazonS3 targetConn, S3Object object, String key) throws Exception {
   		List<String> lines = new ArrayList<>();

   		File tempFile = null;
   		InputStream is = object.getObjectContent();
   		OutputStream os = null;
   		TransferManager tm = new TransferManager(targetConn);
   		try {
   	   		tempFile = new File(tempDir, UUID.randomUUID().toString());
   			os = new FileOutputStream(tempFile);
   			//logger.info("Downloading an object... from bucketName:{} key:{}", object.getBucketName(), key);
   			StreamUtils.copy(is, os);
   		} catch (Exception e) {
   			lines.add(key + " - " + e.getMessage());
   			org.apache.commons.io.FileUtils.writeLines(errorFile, lines, true);
   			throw e;
   		} finally {
   			if (is != null) { is.close(); }
   			if (os != null) { os.close(); }
   		}
   		
   		try {   			
   			logger.debug("uploading an object... to targetBucketName= " + targetBucketName + ", key = " + key);
			PutObjectRequest putObjectRequest = new PutObjectRequest(targetBucketName, key, tempFile);
			putObjectRequest.setCannedAcl(CannedAccessControlList.PublicRead);
			Upload myUpload = tm.upload(putObjectRequest);
			myUpload.waitForCompletion();
			
	   		lines.add(key);
	   		org.apache.commons.io.FileUtils.writeLines(successFile, lines, true);
   		} catch (Exception e) {
   			lines.add(key + " - " + e.getMessage());
   			org.apache.commons.io.FileUtils.writeLines(errorFile, lines, true);
   			logger.error(e.getMessage());
		} finally{	
			if (tempFile != null) { tempFile.delete(); }
		}
	}
}
