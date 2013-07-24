package com.movix.admin_core.scheduling.cache;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.movix.admin_core.scheduling.model.dao.EventDAO;
import com.movix.admin_core.scheduling.model.dao.EventDAOFactory;
import com.movix.admin_core.scheduling.model.dto.EventDTO;

public class EventCache {

	final private String everythingKey = "Everything";
	LoadingCache<String, List<EventDTO>> cache;
	private EventDAO eventDAO;
	private Date lastUpdate;
	
	public void init(){
		cache = CacheBuilder.newBuilder().
		expireAfterAccess(1, TimeUnit.MINUTES).
		removalListener(
			new RemovalListener<String, List<EventDTO>>(){
				public void onRemoval(RemovalNotification<String, List<EventDTO>> notification) {
					System.out.println("SchedulingCache: Se removerá el arreglo");
					if(notification.getCause() == RemovalCause.EXPIRED){
						System.out.println("SchedulingCache: La siguiente entrada expiró: " + notification.getKey());	                                    	
					}
					else{
						System.out.println("SchedulingCache: La siguiente entrada fue borrada intencionalmente: " + notification.getCause());
					}
					refreshCache();
				}
			}
		).
		build(
			new CacheLoader<String, List<EventDTO>>() {
				public List<EventDTO> load(String key) {
					eventDAO = EventDAOFactory.getEventDAO();
					return eventDAO.findAll();
				}
			}
		);
	}
	
	public List<EventDTO> getAll(){
		List<EventDTO> events = new ArrayList<EventDTO>();
		try {
			events = cache.get(everythingKey);
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return events;
//		eventDAO = EventDAOFactory.getEventDAO();
//		return eventDAO.findAll();
	}

	public EventDTO getEvent(Integer eventId){
		List<EventDTO> events = new ArrayList<EventDTO>();
		try {
			events = this.get(everythingKey);
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		eventDAO = EventDAOFactory.getEventDAO();
//		List<EventDTO> events = eventDAO.findAll();
		for(EventDTO event : events){
			if(event.getId() == eventId){
				return event;
			}
		}
		return null;
	}

	public void updateEvent(EventDTO event){
		eventDAO = EventDAOFactory.getEventDAO();
		eventDAO.upsert(event);
		this.invalidateEventCache();
//		cache.put(everythingKey, eventDAO.findAll());
	}

	public List<EventDTO> get(String key) throws ExecutionException{
		return cache.get(key);
	}

	public Date getLastUpdate(){
		return lastUpdate;
	}

	private void refreshCache(){
		lastUpdate = new Date();
		cache.refresh(everythingKey);
	}

	public void invalidateEventCache(){
		lastUpdate = new Date();
		cache.invalidate(everythingKey);
	}
	
	public void test(){
		eventDAO.test();
	}
}
