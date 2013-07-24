package com.movix.admin_core.scheduling.model.dao;

public class EventDAOFactory {

	public static EventDAO getEventDAO(){
		return new EventDAOImpl();
	}
}
