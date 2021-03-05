package de.komoot.photon;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.komoot.photon.lucene.LuceneBackend;
import de.komoot.photon.lucene.LuceneSearcher;
import de.komoot.photon.nominatim.NominatimConnector;
import de.komoot.photon.nominatim.NominatimUpdater;
import de.komoot.photon.utils.CorsFilter;
import lombok.extern.slf4j.Slf4j;

import static spark.Spark.*;


@Slf4j
public class App {

    public static void main(String[] rawArgs) throws Exception {
        // parse command line arguments
        CommandLineArgs args = new CommandLineArgs();
        final JCommander jCommander = new JCommander(args);
        try {
            jCommander.parse(rawArgs);
            if (args.isCorsAnyOrigin() && args.getCorsOrigin() != null) { // these are mutually exclusive
                throw new ParameterException("Use only one cors configuration type");
            }
        } catch (ParameterException e) {
            log.warn("could not start photon: " + e.getMessage());
            jCommander.usage();
            return;
        }

        // show help
        if (args.isUsage()) {
            jCommander.usage();
            return;
        }


        LuceneBackend backend = new LuceneBackend(args);

        if (args.isRecreateIndex()) {
            startRecreatingIndex(backend);
            return;
        }

        if (args.isNominatimImport()) {
            startNominatimImport(backend, args);
            return;
        }

        if (args.isNominatimUpdate()) {
            startNominatimUpdater(backend, args);
            return;
        }

        // no special action specified -> normal mode: start search API
        startApi(backend, args);
    }


    private static void startRecreatingIndex(LuceneBackend backend) {
        backend.recreateIndex();

        log.info("deleted photon index and created an empty new one.");
    }




    /**
     * take nominatim data to fill elastic search index

     */
    private static void startNominatimImport(LuceneBackend backend, CommandLineArgs args) {
        backend.recreateIndex(); // dump previous data

        log.info("starting import from nominatim to photon with languages: " + args.getLanguages());
        de.komoot.photon.Importer importer = backend.getImporter();
        NominatimConnector nominatimConnector = new NominatimConnector(args.getHost(), args.getPort(), args.getDatabase(), args.getUser(), args.getPassword());
        nominatimConnector.setImporter(importer);
        nominatimConnector.readEntireDatabase(args.getCountryCodes().split(","));

        log.info("imported data from nominatim to photon with languages: " + args.getLanguages());
    }

    /**
     * Prepare Nominatim updater

     */
    private static void startNominatimUpdater(LuceneBackend backend, CommandLineArgs args) {
        NominatimUpdater nominatimUpdater = new NominatimUpdater(args.getHost(), args.getPort(), args.getDatabase(), args.getUser(), args.getPassword());
        Updater updater = backend.getUpdater();
        nominatimUpdater.setUpdater(updater);
        nominatimUpdater.update();
    }

    /**
     * start api to accept search requests via http
     *
     */
    private static void startApi(LuceneBackend backend, CommandLineArgs args) {
        port(args.getListenPort());
        ipAddress(args.getListenIp());

        String allowedOrigin = args.isCorsAnyOrigin() ? "*" : args.getCorsOrigin();
        if (allowedOrigin != null) {
            CorsFilter.enableCORS(allowedOrigin, "get", "*");
        } else {
            before((request, response) -> {
                response.type("application/json; charset=UTF-8"); // in the other case set by enableCors
            });
        }

        LuceneSearcher backend_searcher = backend.getSearcher();

        // setup search API
        get("api", new SearchRequestHandler("api", backend_searcher, args.getLanguages(), args.getDefaultLanguage()));
        get("api/", new SearchRequestHandler("api/", backend_searcher, args.getLanguages(), args.getDefaultLanguage()));
        get("reverse", new ReverseSearchRequestHandler("reverse", backend_searcher, args.getLanguages(), args.getDefaultLanguage()));
        get("reverse/", new ReverseSearchRequestHandler("reverse/", backend_searcher, args.getLanguages(), args.getDefaultLanguage()));

/*        // setup update API
        final NominatimUpdater nominatimUpdater = setupNominatimUpdater(args, esNodeClient);
        get("/nominatim-update", (Request request, Response response) -> {
            new Thread(() -> nominatimUpdater.update()).start();
            return "nominatim update started (more information in console output) ...";
        });*/
    }
}
