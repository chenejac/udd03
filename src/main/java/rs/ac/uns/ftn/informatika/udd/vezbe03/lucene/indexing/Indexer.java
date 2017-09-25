package rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Update;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.DocumentHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.PDFHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.TextDocHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.Word2007Handler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.WordHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.model.IndexUnit;


public class Indexer {
	

	private JestClient client;
	
	private static Indexer indexer = new Indexer();
	
	public static Indexer getInstance(){
		return indexer;
	}
	
	private Indexer(String address, int port) {
		JestClientFactory factory = new JestClientFactory();
		 factory.setHttpClientConfig(new HttpClientConfig
		                        .Builder(address + ":" + port)
		                        .multiThreaded(true)
		                        .build());
		client = factory.getObject();
	}
	
	private Indexer() {
		this("http://localhost", 9200);
	}
	
	
	public boolean delete(String filename){
		JestResult result;
		try {
			result = client.execute(new Delete.Builder(filename)
			        .index("digitallibrary")
			        .type("book")
			        .build());
			if(result.isSucceeded())
				return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public boolean add(IndexUnit unit){
		Index index = new Index.Builder(unit).index("digitallibrary").type("book").build();
		JestResult result;
		try {
			result = client.execute(index);
			return result.isSucceeded();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	public boolean updateDocument(String filename, IndexUnit unit) throws InterruptedException, ExecutionException{		
		try {
			JestResult result = client.execute(new Update.Builder(unit).index("digitallibrary").type("book").id(filename).build());
			return result.isSucceeded();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 
	 * @param file Direktorijum u kojem se nalaze dokumenti koje treba indeksirati
	 */
	public int index(File file){		
		DocumentHandler handler = null;
		String fileName = null;
		Bulk.Builder bulkBuilder = new Bulk.Builder()
				.defaultIndex("digitallibrary")
				.defaultType("book");
		
		BulkResult response = null;
		int retVal = 0;
		try {
			File[] files;
			if(file.isDirectory()){
				files = file.listFiles();
			}else{
				files = new File[1];
				files[0] = file;
			}
			for(File newFile : files){
				if(newFile.isFile()){
					fileName = newFile.getName();
					handler = getHandler(fileName);
					if(handler == null){
						System.out.println("Nije moguce indeksirati dokument sa nazivom: " + fileName);
						continue;
					}					
					bulkBuilder.addAction(new Index.Builder(handler.getIndexUnit(newFile)).build());
				} else if (newFile.isDirectory()){
					retVal += index(newFile);
				}
			}
			Bulk bulk = bulkBuilder.build();
			response = client.execute(bulk);
			System.out.println("indexing done");
		} catch (IOException e) {
			System.out.println("indexing NOT done");
		}
		if(!response.isSucceeded()){
			return -1;
		}else{ 
			retVal += response.getItems().size();
			return retVal;
		}
	}
	
	protected void finalize() throws Throwable {
		this.client.shutdownClient();
	}
	
	public DocumentHandler getHandler(String fileName){
		if(fileName.endsWith(".txt")){
			return new TextDocHandler();
		}else if(fileName.endsWith(".pdf")){
			return new PDFHandler();
		}else if(fileName.endsWith(".doc")){
			return new WordHandler();
		}else if(fileName.endsWith(".docx")){
			return new Word2007Handler();
		}else{
			return null;
		}
	}

}