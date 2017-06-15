package com.xiyuan.screenshot.task;

import com.google.gson.JsonElement;
import com.xiyuan.spider.annotation.AddToQueue;
import com.xiyuan.spider.annotation.OnMessage;
import com.xiyuan.spider.annotation.OnStart;
import com.xiyuan.spider.annotation.Task;
import com.xiyuan.spider.filter.BloonFilter;
import com.xiyuan.spider.message.DefaultMessage;
import com.xiyuan.spider.queue.DefaultQueue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiyuan_fengyu on 2017/6/15.
 */
@Task
public class ProxyTask {

    /**
     * url 和 js 保持空字符串, 这样任务实际只执行这个回调函数的内容，不会派发任务到phantomjs
     */
    @OnStart(name = "读取代理地址列表，创建动态代理任务", url = "", js = "")
    @AddToQueue(name = "proxyTasks", type = DefaultQueue.class, filter = BloonFilter.class)
    public ArrayList<DefaultMessage> createProxyTasks(String url, JsonElement result) {
        ArrayList<DefaultMessage> tasks = new ArrayList<>();

        //读取代理列表
        List<String> proxys = null;
        try {
             proxys = Files.readAllLines(Paths.get(ProxyTask.class.getClassLoader().getResource("data/proxys.data").toURI()), StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (proxys != null) {
            //假设一个代理只能访问2次
            for (String proxy : proxys) {
                for (int i = 0; i < 2; i++) {
                    DefaultMessage taskMsg = new DefaultMessage("https://www.baidu.com");
                    taskMsg.setProxy(proxy);
                    tasks.add(taskMsg);
                }
            }
        }

        return tasks;
    }

    @OnMessage(name = "执行动态代理任务", fromQueue = "proxyTasks", js = "js/ProxyTask.js", parallel = 3)
    public void result(String url, JsonElement result) {
        System.out.println(url + "\n" + result);
    }

    public void onError(String url, JsonElement error) {
        System.out.println(url + "\n" + error);
    }

}
