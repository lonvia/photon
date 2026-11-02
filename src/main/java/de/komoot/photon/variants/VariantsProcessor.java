package de.komoot.photon.variants;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.normalization.Normalizer;
import de.komoot.photon.normalization.NullNormalizer;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

@NullMarked
public class VariantsProcessor {
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
                if (data == null) {
                    data = new ArrayList<>();
                }
                data.add(transformer);
            } else {
                if (children == null) {
                    children = new HashMap<>();
                }
                var attr = filterAttributes.get(pos);
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
    private final Normalizer normalizer;
    private final List<Function<PhotonDoc, @Nullable String>> photonDocFilters;
    private final List<Function<String, @Nullable String>> nameFilters;
    @Nullable private TreeNode transformers;

    public VariantsProcessor(
            Set<String> languages,
            Normalizer normalizer,
            List<Function<PhotonDoc, @Nullable String>> photonDocFilters,
            List<Function<String, @Nullable String>> nameFilters) {
        this.languages = languages;
        this.normalizer = normalizer;
        this.photonDocFilters = photonDocFilters;
        this.nameFilters = nameFilters;
    }

    public void addTransformer(List<@Nullable String> filterAttributes, WordTransformer transformer) {
        if (transformers == null) {
            transformers = new TreeNode();
        }
        transformers.addData(filterAttributes, 0, transformer);
    }

    public Transform getTransformForDoc(PhotonDoc doc) {
        if (transformers == null) {
            return new Transform();
        }

        var finalTree = transformers;
        for (var filt : photonDocFilters) {
            finalTree = finalTree.getChild(filt.apply(doc));
            if (finalTree == null) {
                return new Transform();
            }
        }
        return new Transform(finalTree);
    }

    public String normalize(String input) {
        return normalizer.transform(input);
    }

    public class Transform {
        @Nullable private final TreeNode tree;

        Transform() {
            this.tree = null;
        }

        Transform(@Nullable TreeNode tree) {
            this.tree = tree;
        }

        public String normalize(String input) {
            return normalizer.transform(input);
        }

        public Set<String> transform(String nameType, String value) {
            var variants = Set.of(normalizer.transform(value));

            if (tree != null) {
                var finalTree = tree;
                var lang = languages.contains(nameType) ? nameType : "default";
                for (var filt : nameFilters) {
                    finalTree = finalTree.getChild(filt.apply(lang));
                    if (finalTree == null) {
                        return variants;
                    }
                }

                if (finalTree.data != null) {
                    for (var trans : finalTree.data) {
                        variants = trans.transform(variants);
                    }
                }
            }

            return variants;
        }
    }

}
