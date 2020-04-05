package jp.co.apasys.model;

import java.util.ArrayList;
import java.util.List;

public class MergeTableInfo {

    private String newTableNameString = "";
    private String newTableKeyColumn = "";
    private String newTableMergeColumn = "";
    private String newTableMergeFilter = "";
    private List<String> newTableKeyColumnList = new ArrayList<String>();
    private List<String> newTableMergeColumnList = new ArrayList<String>();
    private String oldTableNameString = "";
    private String oldTableKeyColumn = "";
    private String oldTableMergeColumn = "";
    private String oldTableMergeFilter = "";
    private List<String> oldTableKeyColumnList = new ArrayList<String>();
    private List<String> oldTableMergeColumnList = new ArrayList<String>();

    public String getNewTableNameString() {
	return newTableNameString;
    }

    public void setNewTableNameString(String newTableNameString) {
	this.newTableNameString = newTableNameString;
    }

    public String getNewTableKeyColumn() {
	return newTableKeyColumn;
    }

    public void setNewTableKeyColumn(String newTableKeyColumn) {
	this.newTableKeyColumn = newTableKeyColumn;
    }

    public String getNewTableMergeColumn() {
	return newTableMergeColumn;
    }

    public void setNewTableMergeColumn(String newTableMergeColumn) {
	this.newTableMergeColumn = newTableMergeColumn;
    }

    public String getNewTableMergeFilter() {
	return newTableMergeFilter;
    }

    public void setNewTableMergeFilter(String newTableMergeFilter) {
	this.newTableMergeFilter = newTableMergeFilter;
    }

    public List<String> getNewTableKeyColumnList() {
	return newTableKeyColumnList;
    }

    public void setNewTableKeyColumnList(List<String> newTableKeyColumnList) {
	this.newTableKeyColumnList = newTableKeyColumnList;
    }

    public List<String> getNewTableMergeColumnList() {
	return newTableMergeColumnList;
    }

    public void setNewTableMergeColumnList(List<String> newTableMergeColumnList) {
	this.newTableMergeColumnList = newTableMergeColumnList;
    }

    public String getOldTableNameString() {
	return oldTableNameString;
    }

    public void setOldTableNameString(String oldTableNameString) {
	this.oldTableNameString = oldTableNameString;
    }

    public String getOldTableKeyColumn() {
	return oldTableKeyColumn;
    }

    public void setOldTableKeyColumn(String oldTableKeyColumn) {
	this.oldTableKeyColumn = oldTableKeyColumn;
    }

    public String getOldTableMergeColumn() {
	return oldTableMergeColumn;
    }

    public void setOldTableMergeColumn(String oldTableMergeColumn) {
	this.oldTableMergeColumn = oldTableMergeColumn;
    }

    public String getOldTableMergeFilter() {
	return oldTableMergeFilter;
    }

    public void setOldTableMergeFilter(String oldTableMergeFilter) {
	this.oldTableMergeFilter = oldTableMergeFilter;
    }

    public List<String> getOldTableKeyColumnList() {
	return oldTableKeyColumnList;
    }

    public void setOldTableKeyColumnList(List<String> oldTableKeyColumnList) {
	this.oldTableKeyColumnList = oldTableKeyColumnList;
    }

    public List<String> getOldTableMergeColumnList() {
	return oldTableMergeColumnList;
    }

    public void setOldTableMergeColumnList(List<String> oldTableMergeColumnList) {
	this.oldTableMergeColumnList = oldTableMergeColumnList;
    }

}
