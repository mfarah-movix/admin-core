package com.movix.adminScheduling.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.movix.admin_core.scheduling.cache.EventCache;
import com.movix.admin_core.scheduling.comparators.OperadorComparator;
import com.movix.admin_core.scheduling.model.dao.EventDAOImpl.Product;
import com.movix.admin_core.scheduling.model.dto.EventDTO;
import com.movix.shared.Operador;
import com.movixla.service.scheduling.common.SchedulingEntryPro;
import com.movixla.service.scheduling.common.SchedulingEntryPro.Type;

@WebServlet("/events")
public class EventsController extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final String INDEX = "/index.jsp";
	private static final String EDIT = "/edit.jsp";
    private static final String E404 = "/404.html";
	private EventCache eventCache;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public EventsController() {
    	super();
    	eventCache = new EventCache();
    	eventCache.init();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String action = request.getParameter("action") == null ? "index" : request.getParameter("action");
		String forward = E404;
		if(action.equals("index") ){
			getIndex(request);
			forward = INDEX;
		} else if (action.equals("edit") || action.equals("add")){
			request.setAttribute("action", action);
			getEdit(request);
			forward = EDIT;
		} else if (action.equals("delete")) {
			getDelete(request);
			getIndex(request);
			forward = INDEX;
		}
		RequestDispatcher view = request.getRequestDispatcher(forward);
		view.forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String action = request.getParameter("action") == null ? "create" : request.getParameter("action");
		String forward = INDEX;
		if(action.equals("add")){
			postAdd(request);
		} else if (action.equals("edit")){
			postEdit(request);
		} else if (action.equals("delete")){
			postDelete(request);
		}
		getIndex(request);
		RequestDispatcher view = request.getRequestDispatcher(forward);
		view.forward(request, response);
	}
	
	private void getIndex(HttpServletRequest request) {
		List<EventDTO> events = eventCache.getAll();
		System.out.println("************************************************");
		System.out.println("************************************************");
		System.out.println("************************************************");
		for(EventDTO event : events){
			for(SchedulingEntryPro entry : event.getEntries()){
				if(entry.getEnd().equals("11:22")){					
					System.out.println("entry" + entry);
				}
			}
		}
		request.setAttribute("events", events);
	}

	private void getEdit(HttpServletRequest request){
		String[] testOperators = {"WAU_DE_WAULANDIA", "DUMMY", "DESCONOCIDO"};
		List<Operador> operators = new ArrayList<Operador>();
		for(Operador operator : Arrays.asList(Operador.values())){
			if(!Arrays.asList(testOperators).contains(operator.name())){
				operators.add(operator);
			}
		}
		Collections.sort(operators, new OperadorComparator());
		Product[] products = Product.values();
		Type[] types = Type.values();
		String[] weekDays = {"LU", "MA", "MI", "JU", "VI", "SA", "DO"};
		String eventIdParam = request.getParameter("eventId");
		EventDTO event = new EventDTO();
		if(eventIdParam != null){
			event = eventCache.getEvent(Integer.parseInt(eventIdParam));
		} else {
			BiMap<String, String> dias = HashBiMap.create();
			dias.put("", "-");
			event.setDias(dias);
		}
		request.setAttribute("event", event);
		request.setAttribute("operators", operators);
		request.setAttribute("products", products);
		request.setAttribute("types", types);
		request.setAttribute("weekDays", weekDays);
	}
	
	private void getDelete(HttpServletRequest request){
		Integer eventId = Integer.parseInt(request.getParameter("eventId"));
		EventDTO event = eventCache.getEvent(eventId);
		event.setActive(false);
		eventCache.updateEvent(event);
	}

	private void postAdd(HttpServletRequest request){
		EventDTO event = getEventFromRequest(request);
		eventCache.updateEvent(event);
	}

	private void postEdit(HttpServletRequest request){
		EventDTO event = getEventFromRequest(request);
		Integer eventId = Integer.parseInt(request.getParameter("eventId"));
		EventDTO oldEvent = eventCache.getEvent(eventId);
		event.setEntries(oldEvent.getEntries());
		eventCache.updateEvent(event);
	}

	private EventDTO getEventFromRequest(HttpServletRequest request) {
		BiMap<String, String> daysMap = HashBiMap.create();
		String rawDaysMap = request.getParameter("daysMap");
		String[] daysMapArray = rawDaysMap.split("/");
		for(int i = 0; i < daysMapArray.length; i++){
			String[] daysMapElement = daysMapArray[i].split("=");
			daysMap.put(daysMapElement[0].trim(), daysMapElement[1].trim());
		}

		String operador = request.getParameter("operador");
		String producto = request.getParameter("producto");
		String sp = request.getParameter("sp");
		String tipo = request.getParameter("tipo").equals("1") ? "CHARGE" : "SENDING";
		boolean anySp = sp.equals("Todos") || sp.equals("");
		
		EventDTO event = new EventDTO();
		String newKey = producto + ":" + Operador.valueOf(operador).getIdBD() + ":" + ( anySp ? "_" : sp ) + ":" + tipo;
		event.setKey(newKey);
		event.setDias(daysMap);
		event.setOperador(Operador.valueOf(operador));
		event.setProducto(producto);
		event.setSp(anySp ? null : sp);
		event.setTipo(tipo);
		event.setActive(true);
		return event;
	}
	
	private void postDelete(HttpServletRequest request){
	}
}
