package com.jessie.knowledge.taskservice.mvc;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.jessie.knowledge.taskservice.model.UrlLink;
import com.jessie.knowledge.taskservice.model.Constant;
import com.jessie.knowledge.taskservice.model.Task;
import com.jessie.knowledge.taskservice.model.TaskParser;
import com.jessie.knowledge.taskservice.model.TaskSearchResult;
import com.jessie.knowledge.taskservice.service.TaskService;
import com.jessie.knowledge.taskservice.service.UrlService;
import com.jessie.knowledge.taskservice.util.MathUtil;

@RestController
public class TaskResource {
	@Autowired
	@Qualifier("taskService")
	private TaskService taskService;

	@Autowired
	@Qualifier("urlService")
	private UrlService urlService;

	private final Logger logger = Logger.getLogger(getClass());

	@RequestMapping(value = "/task/{taskId}", method = RequestMethod.GET)
	public Task getTask(@PathVariable("taskId") String id) {
		Task t = taskService.getTaskById(new Integer(id));
		return t;
	}

	@RequestMapping(value = "/task", method = RequestMethod.POST)
	public int setTask(@RequestBody Task t) {
		logger.info("create task-" + t.getTaskId() + " request");
		List<TaskParser> tps = t.getParser();
		Iterator itr = tps.iterator();
		while (itr.hasNext()) {
			TaskParser tp = (TaskParser) itr.next();
			tp.setTask(t);
		}
		taskService.createTask(t);
		urlService.create(t, true);

		return t.getTaskId();
	}

	@RequestMapping(value = "/task", method = RequestMethod.GET)
	public TaskSearchResult searchTask(
			@RequestParam(value = "q", defaultValue = "") String searchStr,
			@RequestParam(value = "qf", defaultValue = "") String searchField,
			@RequestParam(value = "sf", defaultValue = "taskId") String sortField,
			@RequestParam(value = "odr", defaultValue = "DESC") String order,
			@RequestParam(value = "limit", defaultValue = "10") Integer limit,
			@RequestParam(value = "offset", defaultValue = "0") Integer offset) {
		logger.info("get tasks request");
		return taskService.searchTask(searchStr, searchField, sortField, order,
				limit, offset);
	}

	@RequestMapping(value = "/task/user", method = RequestMethod.GET)
	public TaskSearchResult getUserTask(Principal user) {
		return taskService.getUserTask(user.getName());

	}
	
	
	// 重置某个任务
	@RequestMapping(value = "/task/{taskid}", method = RequestMethod.PUT)
	public String deleteTask(@PathVariable("taskid") String taskid,
			@RequestParam(value = "opt", defaultValue = "") String opt,
			HttpServletRequest request) throws IOException {
		String remoteHost = request.getRemoteHost();
		logger.info(remoteHost + " task-" + taskid + " reset request, option is:" + opt);
		
		Task task = taskService.getTaskById(new Integer(taskid));
		if (null == task) {
			logger.error("There are no task-" + taskid + " exist");
			return null;
		}
		
		urlService.delete(task);
		return "OK";
	}
	
	
	

	// 获取待抓取的URL
	@RequestMapping(value = "/task/{taskid}/url", method = RequestMethod.GET)
	public List<String> getUrlList(@PathVariable("taskid") String taskid,
			HttpServletRequest request) throws IOException {
		String remoteHost = request.getRemoteHost();
		logger.info(remoteHost + " task-" + taskid + " get url list request");

		if (!taskid.equals("3")) {
			return null;
		}

		Task task = taskService.getTaskById(new Integer(taskid));
		if (null == task) {
			logger.error("There are no task-" + taskid + " exist");
			return null;
		}
		int count = task.getBufferCount();
		return urlService.getUrlList(task, count);
	}

	// 添加新解析的URL列表
	@RequestMapping(value = "/task/{taskid}/url", method = RequestMethod.POST)
	public boolean putUrlList(@PathVariable("taskid") String taskid, @RequestBody List<UrlLink> urlLinkList,
			HttpServletRequest request) throws IOException {
		String remoteHost = request.getRemoteHost();
		logger.info(remoteHost + " task-" + taskid +"　put url links request");
		
		Task task = taskService.getTaskById(new Integer(taskid));
		if (null == task) {
			logger.error("There are no task-" + taskid + " exist");
			return false;
		}
		if (urlLinkList.size() == 0) {
			logger.warn("There are no urlListList in task-" + taskid);
			return false;
		}
		
		for (UrlLink urlLink : urlLinkList) {
			urlService.putUrlList(task, urlLink.getUrl(), urlLink.getLinkList());
		}
		//logger.info("task-" + taskid + " put url links success!");
		return true;
	}

	// 添加新种子
	@RequestMapping(value = "/task/{taskid}/url/seed", method = RequestMethod.POST)
	public boolean putSeedUrl(@PathVariable("taskid") String taskid,
			@RequestBody List<String> seedList, HttpServletRequest request) throws IOException {
		String remoteHost = request.getRemoteHost();
		logger.info(remoteHost + " task-" + taskid + " put seed url request");

		Task task = taskService.getTaskById(new Integer(taskid));
		if (null == task) {
			logger.error("There are no task-" + taskid + " exist");
			return false;
		}
		return urlService.addSeedList(task, seedList, Constant.ROOT_DEPTH);
	}

	// 查看任务当前的状态
	@RequestMapping(value = "/task/{taskid}/status/detail", method = RequestMethod.GET)
	public String getTaskStatusDetailInfo(@PathVariable("taskid") String taskid, HttpServletRequest request)
			throws IOException {
		String remoteHost = request.getRemoteHost();
		logger.info(remoteHost + " task-" + taskid + " get task info request");

		Task task = taskService.getTaskById(new Integer(taskid));
		if (null == task) {
			logger.error("There are no task-" + taskid + " exist");
			return Constant.ERROR_TASK_INFO;
		}

		return urlService.getTaskStatusInfo(task, true);
	}
	
	// 查看任务当前的状态
	@RequestMapping(value = "/task/{taskid}/status", method = RequestMethod.GET)
	public String getTaskStatusBaseInfo(@PathVariable("taskid") String taskid, HttpServletRequest request)
			throws IOException {
		String remoteHost = request.getRemoteHost();

		logger.info(remoteHost + " task-" + taskid + " get task info request");
		
		Task task = taskService.getTaskById(new Integer(taskid));
		if (null == task) {
			logger.error("There are no task-" + taskid + " exist");
			return Constant.ERROR_TASK_INFO;
		}

		return urlService.getTaskStatusInfo(task, false);
	}	
	
	
	
	
	
	
	// 测试写文件
	@RequestMapping(value = "/task/{taskid}/testw", method = RequestMethod.POST)
	public boolean testMapdbWrite(@PathVariable("taskid") String taskid, HttpServletRequest request) throws IOException {

		Task task = taskService.getTaskById(new Integer(taskid));
		if (null == task) {
			logger.error("There are no task-" + taskid + " exist");
			return false;
		}
		List<String> seedList = new ArrayList<String>();
		Random random = new Random(System.currentTimeMillis());
		int begin = random.nextInt();
		logger.info("begin: " + begin);
		for (int i=begin; i<=(begin+20); i++) {
			if (i % 20 == 0) {
				logger.info("test write " + i + "-th!");
			}
			seedList.clear();
			seedList.add("http://bj.nuomi.com/cinema/cinema_" + i);
			urlService.addSeedList(task, seedList, Constant.ROOT_DEPTH);
		}
	
		return true;
	}
	
	
	
	// 测试读文件
	@RequestMapping(value = "/task/{taskid}/testr", method = RequestMethod.GET)
	public List<String> testMapdbRead(@PathVariable("taskid") String taskid,
			HttpServletRequest request) throws IOException {
		
		Task task = taskService.getTaskById(new Integer(taskid));
		if (null == task) {
			logger.error("There are no task-" + taskid + " exist");
			return null;
		}
		int count = task.getBufferCount();
		List<String> list = null;

		for (int i=1; i<=20; i++) {
			if (i % 20 == 0) {
				logger.info("test write " + i + "-th!");
			}
			list = urlService.getUrlList(task, 1);
		}
		return list;
	}
	
	
	
	// 对于抓取失败的url再放入抓取列表中去重新抓取
	
}
