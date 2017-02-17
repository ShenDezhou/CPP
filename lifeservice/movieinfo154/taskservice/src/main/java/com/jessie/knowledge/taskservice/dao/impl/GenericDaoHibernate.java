package com.jessie.knowledge.taskservice.dao.impl;

import org.hibernate.SessionFactory;
import org.hibernate.HibernateException;
import org.hibernate.Session;

/**
 * This class serves as the Base class for all other DAOs - namely to hold
 * common CRUD methods that they might all use. You should only need to extend
 * this class when your require custom CRUD logic.
 * <p/>
 * <p>To register this class in your Spring context file, use the following XML.
 * <pre>
 *      &lt;bean id="fooDao" class="com.jessie.knowledge.passport.dao.impl.GenericDaoHibernate"&gt;
 *          &lt;constructor-arg value="com.jessie.model.Foo"/&gt;
 *      &lt;/bean&gt;
 * </pre>
 * */
public abstract class GenericDaoHibernate {
	
	private SessionFactory sessionFactory;
	
	public GenericDaoHibernate(){
		
	}
	
	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
	
	public Session getSession() throws HibernateException {
        Session sess = getSessionFactory().getCurrentSession();
        if (sess == null) {
            sess = getSessionFactory().openSession();
        }
        return sess;
    }
}
