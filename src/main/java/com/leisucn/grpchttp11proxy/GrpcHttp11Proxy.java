package com.leisucn.grpchttp11proxy;

import com.google.api.AnnotationsProto;
import com.google.api.HttpRule;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.*;

/**
 * Grpc服务Http1.1代理
 *
 * @Author marksu
 * @Date 2018/7/2
 */
@Slf4j
public class GrpcHttp11Proxy {

    private final Cache<String, GrpcCaller> callCache;

    private final ServiceLoader loader;

    public GrpcHttp11Proxy(ServiceLoader loader) {
        this.callCache = CacheBuilder.newBuilder()
                .build();
        this.loader = loader;
    }

    /**
     * 获取grpc服务的proto文件
     *
     * @param service
     * @return
     */
    public static Descriptors.FileDescriptor getProtoDescriptor(BindableService service) {
        String serviceName = getServiceName(service);
        // option (google.api.http) 需要从proto文件读取
        // XXX服务对应的proto文件抽象为XXXProto
        String protoClzName = String.format("%s.%sProto", getProtoPackageName(service), serviceName);
        // 加载XXXProto类并获取proto文件描述
        try {
            Class protoClz = Class.forName(protoClzName);
            Descriptors.FileDescriptor fileDescriptor =
                    (Descriptors.FileDescriptor) protoClz.getMethod("getDescriptor").invoke(null, (Object[]) null);
            return fileDescriptor;
        } catch (Exception e) {
            log.debug("加载Proto类文件失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 获取服务名称(不含包名)
     *
     * @param service
     * @return
     */
    public static String getServiceName(BindableService service) {
        String serviceName =
                service.bindService().getServiceDescriptor().getName();
        // 截断包名
        serviceName = serviceName.substring(serviceName.lastIndexOf('.') + 1);
        return serviceName;
    }

    /**
     * 获取Proto类的包名
     * 现在只支持 XXX extends XXXGrpc$XXXImplBase的继承关系
     *
     * @param service
     * @return
     */
    public static String getProtoPackageName(BindableService service) {
        Class base = service.getClass().getSuperclass();
        return base.getPackage().getName();
    }

    public static GrpcCaller getGrpcMethodCaller(Descriptors.FileDescriptor protoDescriptor, BindableService service, String uri) {
        String serviceName = getServiceName(service);
        Descriptors.ServiceDescriptor serviceDescriptor =
                protoDescriptor.findServiceByName(serviceName);
        Descriptors.MethodDescriptor targetMethod = null;
        // 根据option (google.api.http) 匹配uri
        for (Descriptors.MethodDescriptor methodDescriptor : serviceDescriptor.getMethods()) {
            DescriptorProtos.MethodOptions options = methodDescriptor.getOptions();
            if (options != null) {
                HttpRule rule = options.getExtension(AnnotationsProto.http);
                if (rule != null && rule.getPatch() != null) {
                    // 当前只匹配POST方法
                    if (rule.getPost().equalsIgnoreCase(uri)) {
                        targetMethod = methodDescriptor;
                        break;
                    }
                }
            } else{ // TODO 如果没有option (google.api.http) 则匹配 package.server/method

            }
        }

        if (targetMethod == null) {
            return null;
        }

        String reqType = targetMethod.getInputType().getName();
        reqType = String.format("%s.%s", getProtoPackageName(service), reqType);
        Method reqMethod = null;
        Method rpcMethod = null;
        try {
            Class reqClz = Class.forName(reqType);
            reqMethod = reqClz.getMethod("newBuilder");
            rpcMethod = service.getClass()
                    .getMethod(toLowerCaseFirstOne(targetMethod.getName()), new Class[]{reqClz, StreamObserver.class});
        } catch (Exception e) {
            log.error("反射方法时错误: {}", e.getMessage());
        }

        // 反射方法失败
        if (rpcMethod == null || reqMethod == null) {
            return null;
        }
        GrpcCaller caller = new GrpcCaller(serviceName);
        caller.setService(service);
        caller.setReqMethod(reqMethod);
        caller.setRpcMethod(rpcMethod);
        return caller;
    }

    public static String toLowerCaseFirstOne(String str) {
        if (Character.isLowerCase(str.charAt(0))) {
            return str;
        }
        return (new StringBuilder()).append(Character.toLowerCase(str.charAt(0))).append(str.substring(1)).toString();
    }

    public Message request(final String uri, String body) {

        log.debug("请求: {}, 参数: {}", uri, body);

        String key = getKeyOfCallFromUri(uri);

        GrpcCaller call = callCache.getIfPresent(key);
        // 为命中缓存则加载Grpc服务
        if (call == null) {
            try {
                call = createCall(uri);
            } catch (Exception e) {
                log.error("创建{}的gRpc请求失败: {}", uri, e.getMessage());
                return null;
            }
        }
        // 创建grpc请求失败
        // TODO 通过Listener参数传递异常
        if (call == null) {
            return null;
        }

        Message message = null;
        try {
            message = call.doCall(body);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    private GrpcCaller createCall(final String uri) throws Exception {
        return callCache.get(uri, () -> {
            GrpcCaller caller = null;
            // 加载全部服务
            List<BindableService> services = loader.load();
            for (BindableService service : services) {
                caller = getGrpcMethodCaller(getProtoDescriptor(service), service, uri);
                if (caller != null) {
                    break;
                }
            }
            return caller;
        });
    }

    /**
     * 从请求URI中获取Grpc Key
     *
     * @param uri
     * @return
     */
    private String getKeyOfCallFromUri(String uri) {
        return uri;
    }

    public static interface ServiceLoader {
        List<BindableService> load();
    }

    /**
     * 封装真正的Grpc请求
     */
    static class GrpcCaller {

        private final String name;

        private final JsonFormat jsonFormat;

        private final Executor executor;

        private BindableService service;

        private Method rpcMethod;

        private Method reqMethod;

        public GrpcCaller(String name) {
            this.name = name;
            this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS
                    , new LinkedBlockingQueue<>()
                    , new ThreadFactoryBuilder()
                    .setNameFormat("gRpcProxy-" + this.name + "-%d")
                    .setPriority(Thread.NORM_PRIORITY)
                    .setDaemon(false)
                    .build());
            jsonFormat = new JsonFormat();
        }

        Message doCall(String req) throws Exception {

            Message.Builder builder =
                    (Message.Builder) reqMethod.invoke(null);
            jsonFormat.merge(req, ExtensionRegistry.getEmptyRegistry(), builder);

            BlockingQueue<ObserverResult> chan = new ArrayBlockingQueue<ObserverResult>(1);

//            this.executor.execute(() -> {
//                try {
//                    rpcMethod.invoke(service, new Object[]{builder.build(), new ChanStreamObserver(chan)});
//                } catch (Exception e) {
//                    chan.add(ObserverResult.newResult(null, e));
//                }
//
//            });

            // 暂时用不到多线程
            rpcMethod.invoke(service, new Object[]{builder.build(), new ChanStreamObserver(chan)});

            ObserverResult result = chan.take();
            if (result.error != null) {
                throw new Exception(result.error);
            }
            return result.message;
        }

        void setService(BindableService service) {
            this.service = service;
        }

        public void setReqMethod(Method reqMethod) {
            this.reqMethod = reqMethod;
        }

        public void setRpcMethod(Method rpcMethod) {
            this.rpcMethod = rpcMethod;
        }
    }

    static class ChanStreamObserver implements StreamObserver<Message> {

        private final BlockingQueue<ObserverResult> chan;
        private Message last;

        public ChanStreamObserver(BlockingQueue<ObserverResult> chan) {
            this.chan = chan;
        }

        @Override
        public void onNext(Message value) {
            this.last = value;
        }

        @Override
        public void onError(Throwable t) {
            this.chan.add(ObserverResult.newResult(last, t));
        }

        @Override
        public void onCompleted() {
            this.chan.add(ObserverResult.newResult(last, null));
        }
    }

    static class ObserverResult {
        final Message message;
        final Throwable error;

        public ObserverResult(Message message, Throwable e) {
            this.message = message;
            this.error = e;
        }

        static ObserverResult newResult(Message message, Throwable e) {
            return new ObserverResult(message, e);
        }
    }
}
