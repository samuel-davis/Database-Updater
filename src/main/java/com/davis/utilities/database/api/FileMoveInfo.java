package com.davis.utilities.database.api;

import java.io.Serializable;
import java.nio.file.Path;

public class FileMoveInfo implements Serializable {
  private String originalParentDir;
  private String originalFilename;
  private String targetParentDir;
  private String targetFileName;

  private Path originalPath;
  private Path targetPathWithOriginalName;
  private Path targetPath;

  private FileMoveInfo() {}

  public FileMoveInfo(FileMoveInfoBuilder builder){
    this.originalFilename = builder.originalFilename;
    this.originalParentDir = builder.originalParentDir;
    this.targetFileName = builder.targetFileName;
    this.targetParentDir = builder.targetParentDir;
    this.originalPath = builder.originalPath;
    this.targetPathWithOriginalName = builder.targetPathWithOriginalName;
    this.targetPath = builder.targetPath;
  }

  public Path getTargetPathWithOriginalName() {
    return targetPathWithOriginalName;
  }

  public void setTargetPathWithOriginalName(Path targetPathWithOriginalName) {
    this.targetPathWithOriginalName = targetPathWithOriginalName;
  }

  public Path getOriginalPath() {
    return originalPath;
  }

  public void setOriginalPath(Path originalPath) {
    this.originalPath = originalPath;
  }

  public Path getTargetPath() {
    return targetPath;
  }

  public void setTargetPath(Path targetPath) {
    this.targetPath = targetPath;
  }

  public String getOriginalParentDir() {
    return originalParentDir;
  }

  public void setOriginalParentDir(String originalParentDir) {
    this.originalParentDir = originalParentDir;
  }

  public String getOriginalFilename() {
    return originalFilename;
  }

  public void setOriginalFilename(String originalFilename) {
    this.originalFilename = originalFilename;
  }

  public String getTargetParentDir() {
    return targetParentDir;
  }

  public void setTargetParentDir(String targetParentDir) {
    this.targetParentDir = targetParentDir;
  }

  public String getTargetFileName() {
    return targetFileName;
  }

  public void setTargetFileName(String targetFileName) {
    this.targetFileName = targetFileName;
  }

public static class FileMoveInfoBuilder{
  private String originalParentDir;
  private String originalFilename;
  private String targetParentDir;
  private String targetFileName;

  private Path originalPath;
  private Path targetPathWithOriginalName;
  private Path targetPath;

  public  FileMoveInfoBuilder(){

  }
  public FileMoveInfoBuilder originalParentDir( String originalParentDir){
    this.originalParentDir = originalParentDir;
    return this;

  }
  public FileMoveInfoBuilder originalFilename( String originalFilename){
    this.originalFilename = originalFilename;
    return this;
  }
  public FileMoveInfoBuilder targetParentDir( String targetParentDir){
    this.targetParentDir = targetParentDir;
    return this;
  }
  public FileMoveInfoBuilder targetFileName( String targetFileName){
    this.targetFileName = targetFileName;
    return this;
  }
  public FileMoveInfoBuilder originalPath( Path originalPath){
    this.originalPath = originalPath;
    return this;
  }
  public FileMoveInfoBuilder targetPathWithOriginalName( Path targetPathWithOriginalName){
    this.targetPathWithOriginalName = targetPathWithOriginalName;
    return this;
  }
  public FileMoveInfoBuilder targetPath( Path targetPath){
    this.targetPath = targetPath;
    return this;
  }
  public FileMoveInfo build(){
   return new FileMoveInfo(this);
  }

}


}
