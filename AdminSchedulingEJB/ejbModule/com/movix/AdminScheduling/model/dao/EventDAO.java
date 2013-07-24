package com.movix.AdminScheduling.model.dao;

import java.util.List;

import com.movix.AdminScheduling.model.dto.EventDTO;

public interface EventDAO {
	
	public void upsert(EventDTO sp);
	
	public List<EventDTO> findAll();
	
	public void test();
}
