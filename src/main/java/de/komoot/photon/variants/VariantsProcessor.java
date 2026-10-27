package de.komoot.photon.variants;

import de.komoot.photon.PhotonDoc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

@NullMarked
public class VariantsProcessor {
    private static final Logger LOGGER = LogManager.getLogger();

    private static class TreeNode {
        @Nullable private static final String ANY = null;
        @Nullable Map<@Nullable String, TreeNode> children;
        @Nullable List<WordTransformer> data;

        @Nullable TreeNode getChild(@Nullable String key) {
            if (children == null) {
                return null;
            }

            var next = children.get(key);

            return next == null ? children.get(ANY) : next;
        }

        void addData(List<@Nullable String> filterAttributes, int pos, WordTransformer transformer) {
            if (pos == filterAttributes.size()) {
                LOGGER.info("Setting data at pos {}", pos);
                if (data == null) {
                    data = new ArrayList<>();
                }
                data.add(transformer);
            } else {
                if (children == null) {
                    children = new HashMap<>();
                }
                var attr = filterAttributes.get(pos);
                LOGGER.info("addData attr = {} at pos {}", attr, pos);
                if (attr == null || attr.isEmpty()) {
                    children.computeIfAbsent(ANY, t -> new TreeNode());
                    for (var t : children.values()) {
                        t.addData(filterAttributes, pos + 1, transformer);
                    }
                } else {
                    children.computeIfAbsent(attr, t -> children.containsKey(ANY) ? children.get(ANY).deepCopy() : new TreeNode())
                            .addData(filterAttributes, pos + 1, transformer);
                }
            }
        }

        private TreeNode deepCopy() {
            var newNode = new TreeNode();
            newNode.data = data == null ? null : new ArrayList<>(data);
            if (children != null) {
                newNode.children = new HashMap<>();
                for (var e : children.entrySet()) {
                    newNode.children.put(e.getKey(), e.getValue().deepCopy());
                }
            }

            return newNode;
        }
    }

    private final Set<String> languages;
    private final List<Function<PhotonDoc, @Nullable String>> photonDocFilters;
    private final List<Function<String, @Nullable String>> nameFilters;
    @Nullable private TreeNode transformers;

    public VariantsProcessor() {
        languages = Set.of();
        photonDocFilters = List.of();
        nameFilters = List.of();
    }

    public VariantsProcessor(
            Set<String> languages,
            List<Function<PhotonDoc, @Nullable String>> photonDocFilters,
            List<Function<String, @Nullable String>> nameFilters) {
        this.languages = languages;
        this.photonDocFilters = photonDocFilters;
        this.nameFilters = nameFilters;
    }

    public void addTransformer(List<@Nullable String> filterAttributes, WordTransformer transformer) {
        if (transformers == null) {
            transformers = new TreeNode();
        }
        LOGGER.info("adding transformer: {}", filterAttributes);
        transformers.addData(filterAttributes, 0, transformer);
    }

    public Transform getTransformForDoc(PhotonDoc doc) {
        if (transformers == null) {
            LOGGER.info("getTransformFromDoc: no transformer");
            return new Transform();
        }

        var finalTree = transformers;
        for (var filt : photonDocFilters) {
            LOGGER.info("getTransformFromDoc: transversing for '{}'", filt.apply(doc));
            finalTree = finalTree.getChild(filt.apply(doc));
            if (finalTree == null) {
                LOGGER.info("getTransformFromDoc: deadlock");
                return new Transform();
            }
        }
        LOGGER.info("getTransformFromDoc: done");
        return new Transform(finalTree);
    }

    public class Transform {
        @Nullable private final TreeNode tree;

        Transform() {
            this.tree = null;
        }

        Transform(@Nullable TreeNode tree) {
            this.tree = tree;
        }

        Set<String> transform(String nameType, String value) {
            var variants = Set.of(value);

            if (tree != null) {
                var finalTree = tree;
                var lang = languages.contains(nameType) ? nameType : "default";
                LOGGER.info("transform: lang = {} ({})", lang, nameType);
                for (var filt : nameFilters) {
                    LOGGER.info("transform: applying {}", filt.apply(lang));
                    finalTree = finalTree.getChild(filt.apply(lang));
                    if (finalTree == null) {
                        LOGGER.info("transform: bailing out");
                        return variants;
                    }
                }

                if (finalTree.data != null) {
                    for (var trans : finalTree.data) {
                        variants = trans.transform(variants);
                    }
                } else {
                    LOGGER.info("transform: done and no transforms");
                }
            }

            return variants;
        }
    }

}
