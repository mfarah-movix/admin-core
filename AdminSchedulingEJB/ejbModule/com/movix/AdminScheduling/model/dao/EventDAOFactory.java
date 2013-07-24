package com.movix.AdminScheduling.model.dao;

public class EventDAOFactory {

	public static EventDAO getEventDAO(){
		return new EventDAOImpl();
	}
}
