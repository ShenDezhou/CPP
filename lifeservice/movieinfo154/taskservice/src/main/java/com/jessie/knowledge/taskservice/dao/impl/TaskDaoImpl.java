package com.jessie.knowledge.taskservice.dao.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.criterion.MatchMode;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.springframework.dao.DataAccessException;

import com.jessie.knowledge.taskservice.dao.TaskDao;
import com.jessie.knowledge.taskservice.model.Task;
import com.jessie.knowledge.taskservice.model.TaskSearchResult;

public class TaskDaoImpl extends GenericDaoHibernate implements TaskDao {
	private final Logger log = Logger.getLogger(getClass());

	protected void initDao() {
		// do nothing
	}

	public void create(Task transientInstance) {
		log.debug("saving Task instance");
		try {
			getSessionFactory().getCurrentSession().save(transientInstance);
			getSessionFactory().getCurrentSession().flush();
			log.debug("save successful");
		} catch (RuntimeException re) {
			log.error("save failed", re);
			throw re;
		}
	}

	public void update(Task instance) {
		try {
			getSessionFactory().getCurrentSession().update(instance);
			getSessionFactory().getCurrentSession().flush();
			log.info("SUCCESS CORE UPDATE taskid: " + instance.getTaskId());
		} catch (DataAccessException e) {
			log.info(e.toString());
			log.info("FAIL CORE UPDATE taskid: " + instance.getTaskId());
		}
	}

	public void delete(Task persistentInstance) {
		log.debug("deleting Task instance");
		try {
			getSessionFactory().getCurrentSession().delete(persistentInstance);
			getSessionFactory().getCurrentSession().flush();
			log.debug("delete successful");
		} catch (RuntimeException re) {
			log.error("delete failed", re);
			throw re;
		}
	}

	public Task findById(java.lang.Integer taskId) {
		log.debug("getting Task instance with id: " + taskId);
		try {
			Task instance = (Task) getSessionFactory().getCurrentSession().get(
					"com.jessie.knowledge.taskservice.model.Task", taskId);
			return instance;
		} catch (RuntimeException re) {
			log.error("get failed", re);
			throw re;
		}
	}

	public List findByProperty(String propertyName, Object value) {
		log.debug("finding Task instance with property: " + propertyName
				+ ", value: " + value);
		try {
			return getSessionFactory().getCurrentSession()
					.createCriteria(Task.class)
					.add(Restrictions.eq(propertyName, value)).list();
		} catch (RuntimeException re) {
			log.error("find by property name(" + propertyName + ")failed", re);
			throw re;
		}
	}

	@Override
	public List findByPropertyRange(String propertyName, Double lowValue,
			Double highValue) {
		// TODO Auto-generated method stub
		log.debug("finding Task instance with property: " + propertyName
				+ ", range: " + lowValue + " - " + highValue);
		try {
			return getSessionFactory()
					.getCurrentSession()
					.createCriteria(Task.class)
					.add(Restrictions
							.between(propertyName, lowValue, highValue)).list();
		} catch (RuntimeException re) {
			log.error("find by property name(" + propertyName + ")failed", re);
			throw re;
		}
	}

	@Override
	public TaskSearchResult searchTask(String searchStr, String searchField,
			String sortField, String order, Integer limit, Integer offset) {
		Criteria c = getSessionFactory().getCurrentSession().createCriteria(
				Task.class);
		// TODO Auto-generated method stub
		try {
			if (order.equals("DESC") && !sortField.isEmpty()) {
				c.addOrder(Order.desc(sortField));
			} else {
				c.addOrder(Order.asc(sortField));
			}
			if (!searchStr.isEmpty() && !searchField.isEmpty()) {
				c.add(Restrictions.like(searchField, searchStr,
						MatchMode.ANYWHERE));
			}
			TaskSearchResult t = new TaskSearchResult();
			t.setTotal(c.list().size());
			if (limit > 100)
				limit = 100;
			c.setFirstResult(offset).setMaxResults(limit);
			t.setRows(c.list());
			return t;
		} catch (RuntimeException re) {
			log.error("search property name(" + searchField + ")failed", re);
			throw re;
		}
	}
}