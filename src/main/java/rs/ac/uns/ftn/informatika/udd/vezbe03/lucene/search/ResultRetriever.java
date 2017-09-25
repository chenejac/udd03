package rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.DocumentHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.PDFHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.TextDocHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.Word2007Handler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.indexing.handlers.WordHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.model.IndexUnit;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.model.RequiredHighlight;
import rs.ac.uns.ftn.informatika.udd.vezbe03.lucene.model.ResultData;


public class ResultRetriever {
	
	private static int maxHits = 10;
	
	private static JestClient client;
	
	static {
		JestClientFactory factory = new JestClientFactory();
		 factory.setHttpClientConfig(new HttpClientConfig
		                        .Builder("http://localhost:9200")
		                        .multiThreaded(true)
		                        .build());
		ResultRetriever.client = factory.getObject();
		
	}
	
	
	public static void setMaxHits(int maxHits) {
		ResultRetriever.maxHits = maxHits;
	}

	public static int getMaxHits() {
		return ResultRetriever.maxHits;
	}

	public static List<ResultData> getResults(org.elasticsearch.index.query.QueryBuilder query,
			List<RequiredHighlight> requiredHighlights) {
		if (query == null) {
			return null;
		}
			
		List<ResultData> results = new ArrayList<ResultData>();

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(query);
		searchSourceBuilder.size(maxHits);	
		
		HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.field("text");
        highlightBuilder.field("keywords");
        highlightBuilder.preTags("<spam style='color:red'>").postTags("</spam>");
        highlightBuilder.fragmentSize(200);
        searchSourceBuilder.highlight(highlightBuilder);
		
		Search search = new Search.Builder(searchSourceBuilder.toString())
                // multiple index or types can be added.
                .addIndex("digitallibrary")
                .addType("book")
                .build();
		
		SearchResult result;
		try {
			result = client.execute(search);
			List<SearchResult.Hit<IndexUnit, Void>> hits = result.getHits(IndexUnit.class);
			
			ResultData rd;
			
			for (SearchResult.Hit<IndexUnit, Void> sd : hits) {
				String highlight = "";
				for (String hf : sd.highlight.keySet() ) {
					for (RequiredHighlight rh : requiredHighlights) {
						if(hf.equals(rh.getFieldName())){
							highlight += sd.highlight.get(hf).toString();
						}
					}
				}
				rd = new ResultData(sd.source.getTitle(), sd.source.getKeywords().toString(), sd.source.getFilename(),
						highlight);
				results.add(rd);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return results;
	}
	
	protected static DocumentHandler getHandler(String fileName){
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
