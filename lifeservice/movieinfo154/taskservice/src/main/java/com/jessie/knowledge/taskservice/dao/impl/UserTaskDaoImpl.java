package com.jessie.knowledge.taskservice.dao.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.criterion.Restrictions;
import org.springframework.dao.DataAccessException;

import com.jessie.knowledge.taskservice.dao.UserTaskDao;
import com.jessie.knowledge.taskservice.model.UserTask;

public class UserTaskDaoImpl extends GenericDaoHibernate implements UserTaskDao {
	private final Logger log = Logger.getLogger(getClass());

	protected void initDao() {
		// do nothing
	}

	public void create(UserTask transientInstance) {
		log.debug("saving UserTask instance");
		try {
			getSessionFactory().getCurrentSession().save(transientInstance);
			getSessionFactory().getCurrentSession().flush();
			log.debug("save successful");
		} catch (RuntimeException re) {
			log.error("save failed", re);
			throw re;
		}
	}

	public void update(UserTask instance) {
		try {
			getSessionFactory().getCurrentSession().update(instance);
			getSessionFactory().getCurrentSession().flush();
			log.info("SUCCESS CORE UPDATE UserTask username: "
					+ instance.getUsername());
		} catch (DataAccessException e) {
			log.info(e.toString());
			log.info("FAIL CORE UPDATE UserTask username:  "
					+ instance.getUsername());
		}
	}

	public void delete(UserTask persistentInstance) {
		log.debug("deleting UserTask instance");
		try {
			getSessionFactory().getCurrentSession().delete(persistentInstance);
			getSessionFactory().getCurrentSession().flush();
			log.debug("delete successful");
		} catch (RuntimeException re) {
			log.error("delete failed", re);
			throw re;
		}
	}

	public UserTask findById(java.lang.Integer id) {
		log.debug("getting UserTask instance with id: " + id);
		try {
			UserTask instance = (UserTask) getSessionFactory()
					.getCurrentSession().get(
							"com.jessie.knowledge.taskservice.model.UserTask",
							id);
			return instance;
		} catch (RuntimeException re) {
			log.error("get failed", re);
			throw re;
		}
	}

	public List findByProperty(String propertyName, Object value) {
		log.debug("finding UserTask instance with property: " + propertyName
				+ ", value: " + value);
		try {
			return getSessionFactory().getCurrentSession()
					.createCriteria(UserTask.class)
					.add(Restrictions.eq(propertyName, value)).list();
		} catch (RuntimeException re) {
			log.error("find by property name(" + propertyName + ")failed", re);
			throw re;
		}
	}
}
