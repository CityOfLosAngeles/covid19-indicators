#!/bin/python
import base64
import github
#import sys
import os 
import s3fs

DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}

def upload_file(
        auth_token,
        repo_name,
        branch,
        local_path,
        repo_path,
        commit_message,
        commit_author=DEFAULT_COMMITTER,
        #repo_file_mode valid values: "100644", "100755", "040000", "160000", "120000"
        repo_file_mode="100644",
        timeout=1800):
    if local_path[0:5].lower()=="s3://":
        fs=s3fs.S3FileSystem(anon=False)
        assert fs.exists(local_path), f"local_path = {local_path} does not exist"
        file=fs.open(local_path,"rb").read()
    else:
        assert os.path.exists(local_path), f"local_path = {local_path} does not exist"
        file=open(local_path,"rb").read()
    #print(auth_token)
    #print(timeout)
    g=github.Github(login_or_token=auth_token,timeout=timeout)
    repo=g.get_repo(repo_name)
    ref=repo.get_git_ref(f"heads/{branch}")
    last_commit=repo.get_git_commit(ref.object.sha)
    file=base64.b64encode(file)
    file=str(file,"ascii")
    blob1=repo.create_git_blob(file,"base64")
    author=github.InputGitAuthor(
            commit_author["name"],
            commit_author["email"])
    treeEl=github.InputGitTreeElement(
            repo_path,repo_file_mode,"blob",sha=blob1.sha)
    tree=repo.create_git_tree([treeEl],last_commit.tree)
    thiscommit=repo.create_git_commit(
        commit_message,tree,
        [last_commit],
        committer=author)
    ref.edit(thiscommit.sha)
