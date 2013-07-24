package com.movix.AdminScheduling.model.dto;

import java.util.List;

import com.google.common.collect.BiMap;
import com.movix.shared.Operador;
import com.movixla.service.scheduling.common.SchedulingEntryPro;

public class EventDTO {
	private int id;
	private Operador operador;
	private String producto;
	private int productId;
	private String sp;
	private String tipo;
	private String key;
	private boolean active;
	BiMap<String, String> dias;
	List<SchedulingEntryPro> entries;

	public EventDTO(){
		
	}
	public EventDTO(int id, Operador operador, String producto, int productId,
			String sp, String tipo, String key, BiMap<String, String> dias, String horario) {
		super();
		this.id = id;
		this.operador = operador;
		this.producto = producto;
		this.productId = productId;
		this.sp = sp;
		this.tipo = tipo;
		this.key = key;
		this.dias = dias;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public Operador getOperador() {
		return operador;
	}
	public void setOperador(Operador operador) {
		this.operador = operador;
	}
	public String getProducto() {
		return producto;
	}
	public void setProducto(String producto) {
		this.producto = producto;
	}
	public int getProductId() {
		return productId;
	}
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public String getSp() {
		return sp;
	}
	public void setSp(String sp) {
		this.sp = sp;
	}
	public String getTipo() {
		return tipo;
	}
	public void setTipo(String tipo) {
		this.tipo = tipo;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}
	public BiMap<String, String> getDias() {
		return dias;
	}
	public void setDias(BiMap<String, String> dias) {
		this.dias = dias;
	}
	public List<SchedulingEntryPro> getEntries() {
		return entries;
	}
	public void setEntries(List<SchedulingEntryPro> entries) {
		this.entries = entries;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EventDTO other = (EventDTO) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
}
