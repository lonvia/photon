package de.komoot.photon;

import de.komoot.photon.lucene.LuceneSearcher;
import de.komoot.photon.query.BadRequestException;
import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.query.PhotonRequestFactory;
import de.komoot.photon.searcher.PhotonRequestHandler;
import de.komoot.photon.utils.ConvertToGeoJson;
import org.json.JSONObject;
import spark.Request;
import spark.Response;
import spark.RouteImpl;

import java.util.Arrays;
import java.util.List;

import static spark.Spark.halt;

/**
 * Created by Sachin Dole on 2/12/2015.
 */
public class SearchRequestHandler extends RouteImpl {
    private static final String DEBUG_PARAMETER = "debug";
    
    private final PhotonRequestFactory photonRequestFactory;
    private final PhotonRequestHandler requestHandler;
    private final ConvertToGeoJson geoJsonConverter;

    SearchRequestHandler(String path, LuceneSearcher searcher, String languages, String defaultLanguage) {
        super(path);
        List<String> supportedLanguages = Arrays.asList(languages.split(","));
        this.photonRequestFactory = new PhotonRequestFactory(supportedLanguages, defaultLanguage);
        this.geoJsonConverter = new ConvertToGeoJson();
        this.requestHandler = new PhotonRequestHandler(searcher, supportedLanguages);
    }

    @Override
    public String handle(Request request, Response response) {
        PhotonRequest photonRequest = null;
        try {
            photonRequest = photonRequestFactory.create(request);
        } catch (BadRequestException e) {
            JSONObject json = new JSONObject();
            json.put("message", e.getMessage());
            halt(e.getHttpStatus(), json.toString());
        }
        List<JSONObject> results = requestHandler.handle(photonRequest);
        JSONObject geoJsonResults = geoJsonConverter.convert(results);
        if (request.queryParams(DEBUG_PARAMETER) != null) {
            JSONObject debug = new JSONObject();
            debug.put("query", new JSONObject(requestHandler.dumpQuery(photonRequest)));
            geoJsonResults.put(DEBUG_PARAMETER, debug);
            return geoJsonResults.toString(4);
        }

        return geoJsonResults.toString();
    }
}