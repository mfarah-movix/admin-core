package com.movix.AdminScheduling.comparators;

import java.util.Comparator;

import com.movix.shared.Operador;

public class OperadorComparator implements Comparator<Operador> {

	@Override
	public int compare(Operador o1, Operador o2) {
		String operador1 = o1.getPais().getCodigo().toUpperCase()+o1.name().split("_")[0].substring(0,1).toUpperCase().concat(o1.name().split("_")[0].substring(1).toLowerCase())+o1.getPais().getCodigo().toUpperCase()+" "+o1.name().split("_")[0].substring(0,1).toUpperCase().concat(o1.name().split("_")[0].substring(1).toLowerCase());
		String operador2 = o2.getPais().getCodigo().toUpperCase()+o2.name().split("_")[0].substring(0,1).toUpperCase().concat(o2.name().split("_")[0].substring(1).toLowerCase())+o2.getPais().getCodigo().toUpperCase()+" "+o2.name().split("_")[0].substring(0,1).toUpperCase().concat(o2.name().split("_")[0].substring(1).toLowerCase());
		return operador1.compareTo(operador2);
	}
}

