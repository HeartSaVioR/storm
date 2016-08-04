/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.submit.command;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.storm.submit.dependency.AetherUtils;
import org.apache.storm.submit.dependency.DependencyResolver;
import org.json.simple.JSONValue;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.graph.Dependency;
import org.sonatype.aether.resolution.ArtifactResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DependencyResolverMain {

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("packages must be presented.");
        }

        String packagesArg = args[0];

        // DO NOT CHANGE THIS TO SYSOUT
        System.err.println("DependencyResolver input - packages: " + packagesArg);

        List<Dependency> dependencies = parsePackageArgs(packagesArg);
        try {
            DependencyResolver resolver = new DependencyResolver("local-repo");

            List<ArtifactResult> artifactResults = resolver.resolve(dependencies);

            Iterable<ArtifactResult> missingArtifacts = filterMissingArtifacts(artifactResults);
            if (missingArtifacts.iterator().hasNext()) {
                printMissingArtifactsToSysErr(missingArtifacts);
                throw new RuntimeException("Some artifacts are not resolved");
            }

            System.out.println(JSONValue.toJSONString(transformArtifactResultToArtifactToPaths(artifactResults)));
            System.out.flush();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Iterable<ArtifactResult> filterMissingArtifacts(List<ArtifactResult> artifactResults) {
        return Iterables.filter(artifactResults, new Predicate<ArtifactResult>() {
            @Override
            public boolean apply(ArtifactResult artifactResult) {
                return artifactResult.isMissing();
            }
        });
    }

    private static void printMissingArtifactsToSysErr(Iterable<ArtifactResult> missingArtifacts) {
        for (ArtifactResult artifactResult : missingArtifacts) {
            System.err.println("ArtifactResult : " + artifactResult + " / Errors : " + artifactResult.getExceptions());
        }
    }

    private static List<Dependency> parsePackageArgs(String packagesArg) {
        List<String> packages = Arrays.asList(packagesArg.split(","));
        List<Dependency> dependencies = new ArrayList<>(packages.size());
        for (String packageOpt : packages) {
            if (packageOpt.trim().isEmpty()) {
                continue;
            }

            dependencies.add(AetherUtils.parseDependency(packageOpt));
        }

        return dependencies;
    }

    private static Map<String, String> transformArtifactResultToArtifactToPaths(List<ArtifactResult> artifactResults) {
        Map<String, String> artifactToPath = new LinkedHashMap<>();
        for (ArtifactResult artifactResult : artifactResults) {
            Artifact artifact = artifactResult.getArtifact();
            artifactToPath.put(AetherUtils.artifactToString(artifact), artifact.getFile().getAbsolutePath());
        }
        return artifactToPath;
    }

}
