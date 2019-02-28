package com.greencomnetworks.franzmanager;

import com.greencomnetworks.franzmanager.resources.LiveMessagesResource;
import com.greencomnetworks.franzmanager.services.*;
import com.greencomnetworks.franzmanager.utils.FUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.*;
import org.glassfish.grizzly.http.util.Header;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.glassfish.grizzly.websockets.WebSocketAddOn;
import org.glassfish.grizzly.websockets.WebSocketEngine;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.logging.LogManager;

public class FranzManagerApi {
    private static final Logger logger = LoggerFactory.getLogger(FranzManagerApi.class);

    private final FranzManagerApiConfig apiConfig;

    public FranzManagerApi() {
        this.apiConfig = FranzManagerApiConfig.fromProperties();
    }

    public FranzManagerApi(FranzManagerApiConfig apiConfig) {
        this.apiConfig = apiConfig;
    }

    public FranzManagerApi start() throws IOException {
        URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").path(apiConfig.basePath).port(apiConfig.apiPort).build();
        ResourceConfig config = new ResourceConfig();

        config.register(LoggingFeature.class);
        config.register(JacksonJaxbJsonProvider.class, MessageBodyReader.class, MessageBodyWriter.class);

        config.packages(this.getClass().getPackage().getName() + ".providers");
        config.packages(this.getClass().getPackage().getName() + ".resources");

        // Add tracing capabilities when run in local
        // Header to set to enable trace:  X-Jersey-Tracing-Accept
        if (StringUtils.equals(System.getenv("ENV"), "LOCAL")) {
            config.addProperties(FUtils.SMap.builder()
                    .put("jersey.config.server.tracing.type", "ON_DEMAND")
                    .put("jersey.config.server.tracing.threshold", "VERBOSE")
                    .build());
        }

        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config, false);

        // Websocket
        WebSocketAddOn webSocketAddOn = new WebSocketAddOn();
        NetworkListener webSocketListener = new NetworkListener("websocket", "0.0.0.0", apiConfig.wsPort);
        webSocketListener.registerAddOn(webSocketAddOn);
        server.addListener(webSocketListener);

        WebSocketEngine.getEngine().register(apiConfig.basePath, "/", LiveMessagesResource.getInstance());

        // Documentation
        ServerConfiguration serverConfiguration = server.getServerConfiguration();
        StaticHttpHandler staticHttpHandler = new StaticHttpHandler("apidoc") {
            @Override
            protected void onMissingResource(Request request, Response response) throws Exception {
                super.onMissingResource(request, response);
            }

            // Fix redirect that don't take into account the context path...
            @Override
            protected boolean handle(String uri, Request request, Response response) throws Exception {
                boolean found = false;

                final File[] fileFolders = docRoots.getArray();
                if (fileFolders == null) {
                    return false;
                }

                File resource = null;

                for (int i = 0; i < fileFolders.length; i++) {
                    final File webDir = fileFolders[i];
                    // local file
                    resource = new File(webDir, uri);
                    final boolean exists = resource.exists();
                    final boolean isDirectory = resource.isDirectory();

                    if (exists && isDirectory) {

                        if (!isDirectorySlashOff() && !uri.endsWith("/")) { // redirect to the same url, but with trailing slash
                            // Append context path
                            String contextPath = request.getContextPath();
                            if(contextPath == null) contextPath = "";
                            response.setStatus(HttpStatus.MOVED_PERMANENTLY_301);
                            response.setHeader(Header.Location,
                                response.encodeRedirectURL(contextPath + uri + "/"));
                            return true;
                        }

                        final File f = new File(resource, "/index.html");
                        if (f.exists()) {
                            resource = f;
                            found = true;
                            break;
                        }
                    }

                    if (isDirectory || !exists) {
                        found = false;
                    } else {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    return false;
                }

                assert resource != null;

                // If it's not HTTP GET - return method is not supported status
                if (!Method.GET.equals(request.getMethod())) {
                    response.setStatus(HttpStatus.METHOD_NOT_ALLOWED_405);
                    response.setHeader(Header.Allow, "GET");
                    return true;
                }

                pickupContentType(response, resource.getPath());

                addToFileCache(request, response, resource);
                sendFile(response, resource);

                return true;
            }
        };
        staticHttpHandler.setFileCacheEnabled(false);

        serverConfiguration.addHttpHandler(staticHttpHandler,
            HttpHandlerRegistration.builder().contextPath(apiConfig.basePath + "/apidoc").urlPattern("/").build());


        ConstantsService.init();
        ZookeeperService.init();
        KafkaConsumerOffsetReader.init();
        KafkaMetricsService.init();
        TopicMetricsService.init();
        BrokersService.init();

        // Set Worker Pool Size
        for (NetworkListener listener : server.getListeners()) {
            listener.getTransport().getWorkerThreadPoolConfig().setCorePoolSize(apiConfig.listenerWorkersSizeCore);
            listener.getTransport().getWorkerThreadPoolConfig().setMaxPoolSize(apiConfig.listenerWorkersSizeMax);
        }

        server.start();

        logger.info("Server started on port {} under {}.", apiConfig.apiPort, apiConfig.basePath);

        return this;
    }

    public static void main(String[] args) {
        try {
            LogManager.getLogManager().reset();
            SLF4JBridgeHandler.install();

            FranzManagerApi api = new FranzManagerApi();

            api.start();
        } catch (Throwable e) {
            logger.error("Couldn't start server", e);
            System.exit(1);
        }
    }
}
