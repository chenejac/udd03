package rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import io.searchbox.annotations.JestId;

public class IndexUnit {

	private String text;
	private String title;
	private List<String> keywords = new ArrayList<String>();
	
	@JestId
	private String filename;
	private String filedate;
	
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public List<String> getKeywords() {
		return keywords;
	}
	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getFiledate() {
		return filedate;
	}
	public void setFiledate(String filedate) {
		this.filedate = filedate;
	}
	
	public Document getLuceneDocument(){
		Document retVal = new Document();
		retVal.add(new TextField("text", text, Store.NO));
		retVal.add(new TextField("title", title, Store.YES));
		for (String keyword : keywords) {
			retVal.add(new TextField("keyword", keyword, Store.YES));
		}
		retVal.add(new StringField("filename", filename, Store.YES));
		retVal.add(new TextField("filedate",filedate,Store.YES));
		return retVal;
	}
	
	public XContentBuilder getXContentBuilder() throws IOException{
		XContentBuilder builder = XContentFactory.jsonBuilder()
			    .startObject()
			        .field("text", text)
			        .field("title", title)
			        .field("filename", filename)
			        .field("filedate", filedate)
			        .field("keyword", Arrays.toString(keywords.toArray()))
			    .endObject();
		return builder;
	}
	
}
