import datetime
import traceback
from functools import wraps

from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_
from marshmallow import Schema, fields, ValidationError, pre_load
from webargs import fields
from webargs.flaskparser import use_args


# configure app
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
db = SQLAlchemy(app)


##### EXCEPTION HANDLING #####


class APIException(Exception):
    def __init__(self, message, status_code, payload):
        super().__init__()
        self.message = message
        self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv["message"] = self.message
        rv["status"] = "error"
        return rv


class InvalidPayload(APIException):
    def __init__(self, message="Invalid payload.", payload=None):
        super().__init__(message=message, status_code=400, payload=payload)


class BusinessException(APIException):
    def __init__(self, message="Business rule constraint not satified.", payload=None):
        super().__init__(message=message, status_code=400, payload=payload)


class NotFoundException(APIException):
    def __init__(self, message="Not Found.", payload=None):
        super().__init__(message=message, status_code=404, payload=payload)


class ServerErrorException(APIException):
    def __init__(self, message="Something went wrong.", payload=None):
        super().__init__(message=message, status_code=500, payload=payload)


@app.errorhandler(ValidationError)
@app.errorhandler(InvalidPayload)
@app.errorhandler(BusinessException)
@app.errorhandler(NotFoundException)
@app.errorhandler(ServerErrorException)
@app.errorhandler(APIException)
def handle_exception(error: APIException):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.errorhandler(404)
def handle_not_found(error):
    return handle_exception(NotFoundException())


@app.errorhandler(422)
def handle_not_found(error, **kwargs):
    message = error.data.get("messages").get("json")
    return handle_exception(InvalidPayload(message=message))


@app.errorhandler(500)
def handle_general_exception(error):
    app.logger.error(f"Unknown Exception: {str(error)}")
    app.logger.debug("".join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__)))
    return handle_exception(ServerErrorException())


##### MODELS #####


class Author(db.Model):  # type: ignore
    id = db.Column(db.Integer, primary_key=True)
    first = db.Column(db.String(80))
    last = db.Column(db.String(80))

    def __init__(
        self,
        first: str,
        last: str,
    ):
        self.first = first
        self.last = last


class Quote(db.Model):  # type: ignore
    id = db.Column(db.Integer, primary_key=True)
    content = db.Column(db.String, nullable=False)
    author_id = db.Column(db.Integer, db.ForeignKey("author.id"))
    author = db.relationship("Author", backref=db.backref("quotes", lazy="dynamic"))
    posted_at = db.Column(db.DateTime)

    def __init__(
        self,
        content: str,
        author_id: int,
        posted_at: datetime = datetime.datetime.utcnow()
    ):
        self.content = content
        self.author_id = author_id
        self.posted_at = posted_at


##### SCHEMAS #####


class AuthorSchema(Schema):
    id = fields.Int(dump_only=True)
    first = fields.Str()
    last = fields.Str()
    formatted_name = fields.Method("format_name", dump_only=True)

    def format_name(self, author):
        return "{}, {}".format(author.last, author.first)


# Custom validator
def must_not_be_blank(data):
    if not data:
        raise ValidationError("Data not provided.")


class QuoteSchema(Schema):
    id = fields.Int(dump_only=True)
    author = fields.Nested(AuthorSchema, validate=must_not_be_blank)
    content = fields.Str(required=True, validate=must_not_be_blank)
    posted_at = fields.DateTime(dump_only=True)

    # Allow client to pass author's full name in request body
    # e.g. {"author': 'Tim Peters"} rather than {"first": "Tim", "last": "Peters"}
    @pre_load
    def process_author(self, data, **kwargs):
        author_name = data.get("author")
        if author_name:
            first, last = author_name.split(" ")
            author_dict = dict(first=first, last=last)
        else:
            author_dict = {}
        data["author"] = author_dict
        return data


author_schema = AuthorSchema()
authors_schema = AuthorSchema(many=True)
quote_schema = QuoteSchema()
quotes_schema = QuoteSchema(many=True, only=("id", "content"))


##### CRUD #####


class CRUDBase:
    def __init__(self, model):
        self.model = model

    def get(self, db, id: int):
        return db.session.query(self.model).filter(self.model.id == id).first()

    def get_multi(self, db, skip: int = 0, limit: int = 5):
        return db.session.query(self.model).offset(skip).limit(limit).all()

    def create(self, db, obj_in):
        db_obj = self.model(**obj_in)  # type: ignore
        db.session.add(db_obj)
        db.session.commit()
        db.session.refresh(db_obj)
        return db_obj


class CRUDAuthor(CRUDBase):
    def get_by_name(self, db, first: str, last: str):
        return (
            db.session.query(self.model)
            .filter(and_(self.model.first == first, self.model.last == last))
            .first()
        )


crud_author = CRUDAuthor(model=Author)


class CRUDQuote(CRUDBase):
    def get_multi_by_author(self, db, author_id: int, skip: int = 0, limit: int = 5):
        return (
            db.session.query(self.model)
            .filter(self.model.author_id == author_id)
            .offset(skip)
            .limit(limit)
            .all()
        )

    def create(self, db, obj_in, author: Author):
        db_obj = self.model(**obj_in, author_id=author.id)  # type: ignore
        db.session.add(db_obj)
        db.session.commit()
        db.session.refresh(db_obj)
        return db_obj


crud_quote = CRUDQuote(model=Quote)


##### DECORATORS #####


def response_schema(schema):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            response = f(*args, **kwargs)
            data = schema.dump(response)
            if isinstance(data, list):
                return jsonify(data)
            return data
        return decorated_function
    return decorator
 

def get_author_by_pk(func):
    @wraps(func)
    def wrapper(pk, *args, **kwargs):
        author = crud_author.get(db, id=pk)
        if not author:
            raise NotFoundException("Author Not Found")
        return func(author, *args, **kwargs)
    return wrapper


def get_quote_by_pk(func):
    @wraps(func)
    def wrapper(pk, *args, **kwargs):
        quote = crud_quote.get(db, id=pk)
        if not quote:
            raise NotFoundException("Quote Not Found")
        return func(quote, *args, **kwargs)
    return wrapper


#### API #####


@app.route("/authors")
@response_schema(authors_schema)
def get_authors():
    authors = crud_author.get_multi(db)
    return authors


@app.route("/authors/<int:pk>")
@get_author_by_pk
@response_schema(author_schema)
def get_author(author):
    return author


@app.route("/quotes/", methods=["GET"])
@response_schema(quotes_schema)
def get_quotes():
    quotes = crud_quote.get_multi(db)
    return quotes


@app.route("/quotes/<int:pk>")
@get_quote_by_pk
@response_schema(quote_schema)
def get_quote(quote):
    return quote


@app.route("/quotes/", methods=["POST"])
@response_schema(quote_schema)
@use_args(quote_schema)
def new_quote(args):
    first, last = args["author"]["first"], args["author"]["last"]
    author = crud_author.get_by_name(db, first=first, last=last)
    if author is None:
        # Create a new author
        author = crud_author.create(db, obj_in=args["author"])
    # Create new quote
    del args["author"]
    quote = crud_quote.create(db, obj_in=args, author=author)
    return quote


if __name__ == "__main__":
    db.create_all()
    app.run(debug=True, port=5000)