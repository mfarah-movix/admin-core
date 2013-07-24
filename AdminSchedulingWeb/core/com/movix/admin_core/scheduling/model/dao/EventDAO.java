package com.movix.admin_core.scheduling.model.dao;

import java.util.List;

import com.movix.admin_core.scheduling.model.dto.EventDTO;

public interface EventDAO {
	
	public void upsert(EventDTO sp);
	
	public List<EventDTO> findAll();
	
	public void test();
}
