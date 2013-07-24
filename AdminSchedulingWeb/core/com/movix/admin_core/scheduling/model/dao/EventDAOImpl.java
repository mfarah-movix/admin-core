package com.movix.admin_core.scheduling.model.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.movix.admin_core.scheduling.comparators.SchedulingEntryProComparator;
import com.movix.admin_core.scheduling.model.dto.EventDTO;
import com.movix.shared.Operador;
import com.movixla.service.scheduling.client.SchedulingClient;
import com.movixla.service.scheduling.common.SchedulingEntryPro;
import com.movixla.service.scheduling.common.SchedulingEntryPro.Type;

public class EventDAOImpl implements EventDAO {
	private Integer eventId = 1;
	private String[] daysPatterns = {"LU", "MA", "MI", "JU", "VI", "SA", "DO"};

	@Override
	public void upsert(EventDTO event) {
		SchedulingClient schedulingClient = SchedulingClient.getInstance();

		List<SchedulingEntryPro> schedulingEntryPros = event.getEntries();
		List<SchedulingEntryPro> newSchedulingEntryPros = new ArrayList<SchedulingEntryPro>();

		for(int i = 0; i < daysPatterns.length; i++){
			for(String key : event.getDias().keySet()){
				String[] days = key.split(",");
				for(int j = 0; j < days.length; j++){
					if(daysPatterns[i].equals(days[j])){
						String schedule = event.getDias().get(key);
						String[] daysSchedule = schedule.split("\\|");
						int h = 0;
						for(int k = 0; k < daysSchedule.length; k++){
							String[] rangesByDay = daysSchedule[k].trim().split(",");
							String lastKey = "-1";
							for(int l = 0; l < rangesByDay.length; l++){
								SchedulingEntryPro newEntry = new SchedulingEntryPro();
								String dayPattern = l == 0 && k == 0 ? daysPatterns[i] : "+" + k + "d";
								String startHour = rangesByDay[l].trim().split("-")[0]; 
								String endHour = rangesByDay[l].trim().split("-")[1];
								String spForKey = event.getSp() == null ? "_" : event.getSp();
								String entryKey = event.getProducto() + ":" + event.getOperador().getIdBD() + ":" + spForKey + ":_:" + 
													"d" + j + "-h" + h + "-" + event.getTipo();
								newEntry.setActive(true);
								newEntry.setDayPattern(dayPattern);
								newEntry.setEndHour(endHour.trim());
								newEntry.setKey(entryKey);
								newEntry.setOperator(event.getOperador().getIdBD());
								newEntry.setParentKey(lastKey);
								newEntry.setProduct(event.getProducto());
								newEntry.setServicePrice(event.getSp());
								newEntry.setStartHour(startHour.trim());
								newEntry.setType(Type.valueOf(event.getTipo()));
								newEntry.setActive(event.isActive());
								newSchedulingEntryPros.add(newEntry);
								lastKey = entryKey;
								h++;
							}
						}
					}
				}
			}
		}

		if(schedulingEntryPros != null){
			System.out.println("=====OLD=====");
			for(SchedulingEntryPro oldEntry : schedulingEntryPros){
				System.out.println(oldEntry.getId() + " - " + oldEntry.getOperator() + ", " + oldEntry.getDayPattern() + ":" + oldEntry.getStart() + " - " + oldEntry.getEnd() + ", " + oldEntry.getKey());
			}
		}
		
		int oldIndex = 0;
		System.out.println("===INSERTING===");
		for(SchedulingEntryPro newEntry : newSchedulingEntryPros){
			if(schedulingEntryPros != null && oldIndex < schedulingEntryPros.size()){
				newEntry.setId(schedulingEntryPros.get(oldIndex).getId());
				oldIndex++;
			}
			System.out.println(newEntry.getId() + " - " + newEntry.getOperator() + ", " + newEntry.getDayPattern() + ":" + newEntry.getStart() + " - " + newEntry.getEnd() + ", " + newEntry.getKey());
			schedulingClient.upsert(newEntry);
		}
		if(schedulingEntryPros != null){
			System.out.println("===DISABLING===");
			for(int i = oldIndex; i < schedulingEntryPros.size(); i++){
				SchedulingEntryPro oldEntry = schedulingEntryPros.get(i);
				oldEntry.setActive(false);
				schedulingClient.upsert(oldEntry);
			}
		}
		schedulingClient.invalidate();
	}
	
	@Override
	public List<EventDTO> findAll(){
		System.out.println("findAll called");
		List<EventDTO> events = new ArrayList<EventDTO>();
		SchedulingClient schedulingClient = SchedulingClient.getInstance();
		List<SchedulingEntryPro> schedulingEntries = schedulingClient.getEntries();
		System.out.println("Entries.size" + schedulingEntries.size());
		Collections.sort(schedulingEntries, new SchedulingEntryProComparator());
		SchedulingEntryPro last = new SchedulingEntryPro();
		String hourSchedule = "", lastDay = "", lastServicePrice = "";
		BiMap<String, String> daysMap = HashBiMap.create(7);
		List<SchedulingEntryPro> eventEntries = new ArrayList<SchedulingEntryPro>();
		boolean first = true;
		for(SchedulingEntryPro entry : schedulingEntries){
			lastServicePrice = last.getServicePrice() == null ? "" : last.getServicePrice();
			if(first){
				last = entry;
				lastDay = entry.getDayPattern();
				first = false;
			} else {
				eventEntries.add(last);				
			}
			if(mustCreateEvent(last.getKey(), entry.getKey())){
				putScheduleInMap(hourSchedule, lastDay, daysMap);
				addNewEvent(events, last, lastServicePrice, daysMap, eventEntries);
				eventEntries = new ArrayList<SchedulingEntryPro>();
				lastDay = entry.getDayPattern();
				hourSchedule = getSchedule(entry);
				daysMap = HashBiMap.create(7);
			} else {
				String dayPattern = entry.getDayPattern();
				if(Arrays.asList(daysPatterns).contains(dayPattern)){
					if(dayPattern.equals(lastDay)){
						hourSchedule += getSchedule(entry);
					} else {
						putScheduleInMap(hourSchedule, lastDay, daysMap);
						hourSchedule = getSchedule(entry);
					}
					lastDay = dayPattern;
				} else {
					int daysOff = Integer.parseInt(dayPattern.substring(1,2));
					int daysPassed = hourSchedule.length() - hourSchedule.replace("|", "").length();
					String schedule = getSchedule(entry);
					boolean newDay = false;
					if(daysOff > daysPassed){
						int daysDiff = daysOff - daysPassed;
						String separator = " ";
						for(int i = 0; i < daysDiff; i++){
							separator += "| ";
						}
						schedule = separator + schedule;
						newDay = true;
					}
					hourSchedule += ( newDay ? "" : ", " ) + schedule;
				}
			}
			last = entry;
		}
		lastDay = last.getDayPattern();
		putScheduleInMap(hourSchedule, lastDay, daysMap);
		lastServicePrice = last.getServicePrice() == null ? "" : last.getServicePrice();
		addNewEvent(events, last, lastServicePrice, daysMap, eventEntries);
		return events;
	}
	
	private String getSchedule(SchedulingEntryPro entry) {
		if(entry.getStart().equals("*") || entry.getEnd().equals("*")){
			return "* - *";
		}
		return  entry.getStart() + " - " + entry.getEnd();
	}

	private void putScheduleInMap(String hourSchedule, String lastDay,
			BiMap<String, String> daysMap) {
		if(!daysMap.containsValue(hourSchedule)){
			daysMap.put(lastDay, hourSchedule);
		} else {
			updateKeyAndPut(hourSchedule, lastDay, daysMap);
		}
	}

	private void addNewEvent(List<EventDTO> events, SchedulingEntryPro last,
			String lastServicePrice, BiMap<String, String> daysMap,
			List<SchedulingEntryPro> eventEntries) {
		EventDTO newEvent = new EventDTO();
		String key = last.getProduct() + ":" + last.getOperator() + ":" + lastServicePrice + ":" + last.getKey().substring(last.getKey().lastIndexOf("-") + 1);
		newEvent.setId(eventId);
		newEvent.setDias(daysMap);
		newEvent.setKey(key);
		newEvent.setOperador(Operador.getOperadorPorIdBD(last.getOperator()));
		newEvent.setProducto(last.getProduct());
		newEvent.setSp(lastServicePrice);
		newEvent.setTipo(last.getKey().substring(last.getKey().lastIndexOf("-") + 1));
		newEvent.setEntries(eventEntries);
		events.add(newEvent);
		eventId++;
	}

	private void updateKeyAndPut(String hourSchedule, String lastDay,
			BiMap<String, String> daysMap) {
		String oldKey = daysMap.inverse().get(hourSchedule);
		String newKey = oldKey + "," + lastDay;
		daysMap.remove(oldKey);
		daysMap.put(newKey, hourSchedule);
	}
	
	private boolean mustCreateEvent(String lastKey, String entryKey){
		boolean different = false;
		String[] lastKeyTokens = lastKey.split(":");
		String[] entryKeyTokens = entryKey.split(":");
		for(int i = 0; i < lastKeyTokens.length; i++){
			if(i == lastKeyTokens.length - 1){
				lastKeyTokens[i] = lastKeyTokens[i].replaceAll("[0-9]","");
				entryKeyTokens[i] = entryKeyTokens[i].replaceAll("[0-9]","");
			}
			if(!lastKeyTokens[i].equals(entryKeyTokens[i])){
				different = true;
			}
		}
		return different;
	}
	
	public enum Product {
		SUS, MP
	}
	
	public void test(){
		SchedulingClient schedulingClient = SchedulingClient.getInstance();
		List<SchedulingEntryPro> schedulingEntries = schedulingClient.getEntries();
		for(SchedulingEntryPro entry : schedulingEntries){
			entry.setActive(true);
			schedulingClient.upsert(entry);
		}
	}
}
