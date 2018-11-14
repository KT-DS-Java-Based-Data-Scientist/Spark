package com.ktds.sql;

import java.io.Serializable;

public class Dessert implements Serializable {

	private static final long serialVersionUID = 9074067729143250405L;		// 고유의 ID (Serialize)

	private final String menuId;
	private final String name;
	private final int price;
	private final int kcal;

	public Dessert(String[] args) {
		menuId = args[0];
		name = args[1];
		price = Integer.parseInt(args[2]);
		kcal = Integer.parseInt(args[3]);
	}

	public String getMenuId() {
		return menuId;
	}

	public String getName() {
		return name;
	}

	public int getPrice() {
		return price;
	}

	public int getKcal() {
		return kcal;
	}

}
