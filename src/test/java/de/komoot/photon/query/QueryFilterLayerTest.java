package de.komoot.photon.query;

import de.komoot.photon.ESBaseTester;
import de.komoot.photon.Importer;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.searcher.PhotonResult;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryFilterLayerTest extends ESBaseTester {
    @BeforeAll
    void setUp(@TempDir Path dataDirectory) throws Exception {
        setUpES(dataDirectory);
        Importer instance = makeImporter();

        int id = 0;

        int[] docRanks = {10, 13, 14, 22}; // state, city * 2, locality
        for (int rank : docRanks) {
            instance.add(List.of(new PhotonDoc(id, "W", ++id, "place", "value")
                            .names(Map.of("name", "berlin"))
                            .centroid(FACTORY.createPoint(new Coordinate(10, 10)))
                            .rankAddress(rank)));
        }

        instance.finish();
        refresh();
    }

    @AfterAll
    @Override
    public void tearDown() throws IOException {
        super.tearDown();
    }

    private List<PhotonResult> searchWithLayers(String... layers) {
        SimpleSearchRequest request = new SimpleSearchRequest();
        request.setQuery("berlin");
        request.addLayerFilters(Arrays.stream(layers).collect(Collectors.toSet()));

        return getServer().createSearchHandler(new String[]{"en"}, 1).search(request);
    }

    private List<PhotonResult> reverse(String... layers) {
        ReverseRequest request = new ReverseRequest();
        request.setLocation(FACTORY.createPoint(new Coordinate(10, 10)));
        request.addLayerFilters(Arrays.stream(layers).collect(Collectors.toSet()));

        return getServer().createReverseHandler(1).search(request);
    }

    @Test
    void testSearchSingleLayer() {
        assertThat(searchWithLayers("city"))
                .hasSize(2)
                .allSatisfy(p -> assertThat(p.get("type")).isEqualTo("city"));
    }

    @Test
    void testSearchMultipleLayers() {
        assertThat(searchWithLayers("city", "locality"))
                .hasSize(3)
                .allSatisfy(p -> assertThat(p.get("type")).isNotEqualTo("state"));
    }

    @Test
    void testReverseSingleLayer() {
        assertThat(reverse("city"))
                .hasSize(2)
                .allSatisfy(p -> assertThat(p.get("type")).isEqualTo("city"));
    }

    @Test
    void testReverseMultipleLayers() {
        assertThat(reverse("city", "locality"))
                .hasSize(3)
                .allSatisfy(p -> assertThat(p.get("type")).isNotEqualTo("state"));
    }
}
